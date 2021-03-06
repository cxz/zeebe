/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.gateway.impl;

import io.zeebe.gateway.api.record.Record;
import io.zeebe.gateway.cmd.BrokerErrorException;
import io.zeebe.gateway.cmd.ClientCommandRejectedException;
import io.zeebe.gateway.cmd.ClientException;
import io.zeebe.gateway.cmd.ClientOutOfMemoryException;
import io.zeebe.gateway.impl.broker.RequestDispatchStrategy;
import io.zeebe.gateway.impl.broker.RoundRobinDispatchStrategy;
import io.zeebe.gateway.impl.broker.cluster.BrokerClusterState;
import io.zeebe.gateway.impl.broker.cluster.BrokerTopologyManagerImpl;
import io.zeebe.gateway.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.protocol.clientapi.ErrorCode;
import io.zeebe.protocol.clientapi.MessageHeaderDecoder;
import io.zeebe.transport.ClientOutput;
import io.zeebe.transport.ClientResponse;
import io.zeebe.transport.RequestTimeoutException;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorTask;
import io.zeebe.util.sched.clock.ActorClock;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;

public class RequestManager extends Actor {
  protected final ClientOutput output;
  protected final BrokerTopologyManagerImpl topologyManager;
  protected final ZeebeObjectMapperImpl objectMapper;
  protected final Duration requestTimeout;
  protected final RequestDispatchStrategy dispatchStrategy;
  protected final long blockTimeMillis;

  public RequestManager(
      final ClientOutput output,
      final BrokerTopologyManagerImpl topologyManager,
      final ZeebeObjectMapperImpl objectMapper,
      final Duration requestTimeout,
      final long blockTimeMillis) {
    this.output = output;
    this.topologyManager = topologyManager;
    this.objectMapper = objectMapper;
    this.requestTimeout = requestTimeout;
    this.blockTimeMillis = blockTimeMillis;
    this.dispatchStrategy = new RoundRobinDispatchStrategy(topologyManager);
  }

  public ActorFuture<Void> close() {
    return actor.close();
  }

  private <R> ResponseFuture<R> executeAsync(final RequestResponseHandler requestHandler) {
    final ActorFuture<Supplier<Integer>> remoteProviderFuture =
        determineRemoteProvider(requestHandler);

    final CompletableActorFuture<ClientResponse> deferredFuture = new CompletableActorFuture<>();

    actor.call(
        () ->
            actor.runOnCompletion(
                remoteProviderFuture,
                (remoteProvider, throwable) -> {
                  if (throwable == null) {
                    final ActorFuture<ClientResponse> responseFuture =
                        output.sendRequestWithRetry(
                            remoteProvider,
                            RequestManager::shouldRetryRequest,
                            requestHandler,
                            requestTimeout);

                    if (responseFuture != null) {
                      actor.runOnCompletion(
                          responseFuture,
                          (response, error) -> {
                            if (error == null) {
                              deferredFuture.complete(response);
                            } else {
                              deferredFuture.completeExceptionally(error);
                            }
                          });
                    } else {
                      deferredFuture.completeExceptionally(
                          new ClientOutOfMemoryException(
                              "Zeebe client is out of buffer memory and cannot make "
                                  + "new requests until memory is reclaimed."));
                    }
                  } else {
                    deferredFuture.completeExceptionally(throwable);
                  }
                }));

    return new ResponseFuture<>(deferredFuture, requestHandler, requestTimeout);
  }

  private static boolean shouldRetryRequest(final DirectBuffer responseContent) {
    final ErrorResponseHandler errorHandler = new ErrorResponseHandler();
    final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    headerDecoder.wrap(responseContent, 0);

    if (errorHandler.handlesResponse(headerDecoder)) {
      errorHandler.wrap(
          responseContent,
          headerDecoder.encodedLength(),
          headerDecoder.blockLength(),
          headerDecoder.version());

      final ErrorCode errorCode = errorHandler.getErrorCode();
      return errorCode == ErrorCode.PARTITION_NOT_FOUND || errorCode == ErrorCode.REQUEST_TIMEOUT;
    } else {
      return false;
    }
  }

  private ActorFuture<Integer> updateTopologyAndDeterminePartition() {
    final CompletableActorFuture<Integer> future = new CompletableActorFuture<>();

    actor.call(
        () -> {
          final long timeout = ActorClock.currentTimeMillis() + requestTimeout.toMillis();
          updateTopologyAndDeterminePartition(future, timeout);
        });
    return future;
  }

  private void updateTopologyAndDeterminePartition(
      final CompletableActorFuture<Integer> future, final long timeout) {
    final ActorFuture<BrokerClusterState> topologyFuture = topologyManager.requestTopology();
    actor.runOnCompletion(
        topologyFuture,
        (topology, throwable) -> {
          final int partition = dispatchStrategy.determinePartition();
          if (partition >= 0) {
            future.complete(partition);
          } else if (ActorClock.currentTimeMillis() > timeout) {
            future.completeExceptionally(
                new ClientException(
                    "Could not determine target partition in time " + requestTimeout));
          } else {
            updateTopologyAndDeterminePartition(future, timeout);
          }
        });
  }

  private ActorFuture<Supplier<Integer>> determineRemoteProvider(
      final RequestResponseHandler requestHandler) {
    final CompletableActorFuture<Supplier<Integer>> supplierFuture = new CompletableActorFuture<>();
    if (requestHandler.addressesSpecificPartition()) {
      // partition already known
      supplierFuture.complete(providerForPartition(requestHandler.getTargetPartition()));
    } else {
      // have to select partition
      final int targetPartition = dispatchStrategy.determinePartition();
      if (targetPartition >= 0) {
        // partition known
        requestHandler.onSelectedPartition(targetPartition);
        supplierFuture.complete(providerForPartition(targetPartition));
      } else {
        // not known -> refresh topology and try again
        final ActorFuture<Integer> partitionFuture = updateTopologyAndDeterminePartition();
        actor.call(
            () ->
                actor.runOnCompletion(
                    partitionFuture,
                    (partition, throwable) -> {
                      if (throwable != null) {
                        supplierFuture.completeExceptionally(throwable);
                      } else if (partition >= 0) {
                        requestHandler.onSelectedPartition(partition);
                        supplierFuture.complete(providerForPartition(partition));
                      } else {
                        supplierFuture.completeExceptionally(
                            new ClientException(
                                "Cannot determine target partition for request. Request was:"
                                    + requestHandler.describeRequest()));
                      }
                    }));
      }
    }
    return supplierFuture;
  }

  public <E> ResponseFuture<E> send(final ControlMessageRequest<E> controlMessage) {
    final ControlMessageRequestHandler requestHandler =
        new ControlMessageRequestHandler(objectMapper, controlMessage);
    return executeAsync(requestHandler);
  }

  public <R extends Record> ResponseFuture<R> send(final CommandImpl<R> command) {
    final CommandRequestHandler requestHandler = new CommandRequestHandler(objectMapper, command);
    return executeAsync(requestHandler);
  }

  private <E> E waitAndResolve(final Future<E> future) {
    try {
      return future.get();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for command result", e);
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof ClientException) {
        throw ((ClientException) cause).newInCurrentContext();
      } else {
        throw new ClientException("Could not make request", e);
      }
    }
  }

  private BrokerProvider providerForRandomBroker() {
    return new BrokerProvider(BrokerClusterState::getRandomBroker);
  }

  private BrokerProvider providerForPartition(final int partitionId) {
    return new BrokerProvider(topology -> topology.getLeaderForPartition(partitionId));
  }

  public static class ResponseFuture<E> implements ActorFuture<E> {
    protected final ActorFuture<ClientResponse> transportFuture;
    protected final RequestResponseHandler responseHandler;
    protected final ErrorResponseHandler errorHandler = new ErrorResponseHandler();
    protected final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    protected final Duration requestTimeout;

    protected E result = null;
    protected ExecutionException failure = null;

    ResponseFuture(
        final ActorFuture<ClientResponse> transportFuture,
        final RequestResponseHandler responseHandler,
        final Duration requestTimeout) {
      this.transportFuture = transportFuture;
      this.responseHandler = responseHandler;
      this.requestTimeout = requestTimeout;
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return transportFuture.isDone();
    }

    protected void ensureResponseAvailable(final long timeout, final TimeUnit unit) {
      if (result != null || failure != null) {
        return;
      }

      try {
        final ClientResponse response = transportFuture.get(timeout, unit);
        final DirectBuffer responsBuffer = response.getResponseBuffer();

        headerDecoder.wrap(responsBuffer, 0);

        if (responseHandler.handlesResponse(headerDecoder)) {
          handleExpectedResponse(response, responsBuffer);
          return;
        } else if (errorHandler.handlesResponse(headerDecoder)) {
          handleErrorResponse(responsBuffer);
          return;
        } else {
          failWith("Unexpected response format");
          return;
        }
      } catch (final ExecutionException e) {
        if (e.getCause() != null && e.getCause() instanceof RequestTimeoutException) {
          failWith("Request timed out (" + requestTimeout + ")", e);
        } else {
          failWith("Could not complete request", e);
        }
        return;
      } catch (final InterruptedException | TimeoutException e) {
        failWith("Could not complete request", e);
        return;
      }
    }

    private void handleErrorResponse(final DirectBuffer responseContent) {
      errorHandler.wrap(
          responseContent,
          headerDecoder.encodedLength(),
          headerDecoder.blockLength(),
          headerDecoder.version());

      final ErrorCode errorCode = errorHandler.getErrorCode();

      if (errorCode != ErrorCode.NULL_VAL) {
        try {
          final String errorMessage = BufferUtil.bufferAsString(errorHandler.getErrorMessage());
          failWith(new BrokerErrorException(errorCode, errorMessage));
          return;
        } catch (final Exception e) {
          failWith(
              new BrokerErrorException(
                  errorCode, "Unable to parse error message from response: " + e.getMessage()));
          return;
        }
      } else {
        failWith("Unknown error during request execution");
        return;
      }
    }

    private void handleExpectedResponse(
        final ClientResponse response, final DirectBuffer responseBuffer) {
      try {
        this.result =
            (E)
                responseHandler.getResult(
                    responseBuffer,
                    headerDecoder.encodedLength(),
                    headerDecoder.blockLength(),
                    headerDecoder.version());

        if (this.result instanceof ReceiverAwareResponseResult) {
          ((ReceiverAwareResponseResult) this.result).setReceiver(response.getRemoteAddress());
        }

        return;
      } catch (final ClientCommandRejectedException e) {
        failWith(e);
        return;
      } catch (final Exception e) {
        failWith("Unexpected exception during response handling", e);
        return;
      }
    }

    protected void failWith(final Exception e) {
      this.failure = new ExecutionException(e);
    }

    protected void failWith(final String message) {
      failWith(
          new ClientException(message + ". Request was: " + responseHandler.describeRequest()));
    }

    protected void failWith(final String message, final Throwable cause) {
      failWith(
          new ClientException(
              message + ". Request was: " + responseHandler.describeRequest(), cause));
    }

    @Override
    public E get() throws InterruptedException, ExecutionException {
      try {
        return get(1, TimeUnit.DAYS);
      } catch (final TimeoutException e) {
        throw new ClientException("Failed to wait for response", e);
      }
    }

    @Override
    public E get(final long timeout, final TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      ensureResponseAvailable(timeout, unit);

      if (result != null) {
        return result;
      } else {
        throw failure;
      }
    }

    @Override
    public void complete(final E value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void completeExceptionally(final String failure, final Throwable throwable) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void completeExceptionally(final Throwable throwable) {
      throw new UnsupportedOperationException();
    }

    @Override
    public E join() {
      try {
        return get();
      } catch (final ExecutionException e) {
        final Throwable cause = e.getCause();

        if (cause instanceof ClientException) {
          final ClientException clientException = (ClientException) cause;
          throw clientException.newInCurrentContext();
        } else {
          throw new ClientException(cause.getMessage(), cause);
        }
      } catch (final InterruptedException e) {
        throw new ClientException(e.getMessage(), e);
      }
    }

    public E join(final long timeout, final TimeUnit unit) {
      try {
        return get(timeout, unit);
      } catch (final ExecutionException e) {
        final Throwable cause = e.getCause();

        if (cause instanceof ClientException) {
          final ClientException clientException = (ClientException) cause;
          throw clientException.newInCurrentContext();
        } else {
          throw new ClientException(cause.getMessage(), cause);
        }
      } catch (final InterruptedException | TimeoutException e) {
        throw new ClientException(e.getMessage(), e);
      }
    }

    @Override
    public void block(final ActorTask onCompletion) {
      transportFuture.block(onCompletion);
    }

    @Override
    public boolean isCompletedExceptionally() {
      if (transportFuture.isDone()) {
        ensureResponseAvailable(1, TimeUnit.SECONDS);
        return failure != null;
      } else {
        return false;
      }
    }

    @Override
    public Throwable getException() {
      return failure;
    }
  }

  private class BrokerProvider implements Supplier<Integer> {
    private final Function<BrokerClusterState, Integer> addressStrategy;
    private int attempt = 0;

    BrokerProvider(final Function<BrokerClusterState, Integer> addressStrategy) {
      this.addressStrategy = addressStrategy;
    }

    @Override
    public Integer get() {
      if (attempt > 0) {
        topologyManager.requestTopology();
      }

      attempt++;

      final BrokerClusterState topology = topologyManager.getTopology();
      if (topology != null) {
        return addressStrategy.apply(topology);
      } else {
        return null;
      }
    }
  }
}
