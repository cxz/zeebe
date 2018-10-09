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
package io.zeebe.client.util;

import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.zeebe.gateway.protocol.GatewayGrpc.GatewayImplBase;
import io.zeebe.gateway.protocol.GatewayOuterClass.BrokerInfo;
import io.zeebe.gateway.protocol.GatewayOuterClass.CancelWorkflowInstanceRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CancelWorkflowInstanceResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.CompleteJobRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CompleteJobResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateJobRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateJobResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateWorkflowInstanceRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateWorkflowInstanceResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.DeployWorkflowRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.DeployWorkflowResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.FailJobRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.FailJobResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.HealthRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.HealthResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.Partition;
import io.zeebe.gateway.protocol.GatewayOuterClass.Partition.PartitionBrokerRole;
import io.zeebe.gateway.protocol.GatewayOuterClass.PublishMessageRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateJobRetriesRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateJobRetriesResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateWorkflowInstancePayloadRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateWorkflowInstancePayloadResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.WorkflowResponseObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class RecordingGatewayService extends GatewayImplBase {

  private final List<GeneratedMessageV3> requests = new ArrayList<>();

  private final Map<Class<? extends GeneratedMessageV3>, RequestHandler> requestHandlers =
      new HashMap<>();

  public RecordingGatewayService() {
    addRequestHandler(HealthRequest.class, r -> HealthResponse.getDefaultInstance());
    addRequestHandler(
        DeployWorkflowRequest.class, r -> DeployWorkflowResponse.getDefaultInstance());
    addRequestHandler(PublishMessageRequest.class, r -> Empty.getDefaultInstance());
    addRequestHandler(CreateJobRequest.class, r -> CreateJobResponse.getDefaultInstance());
    addRequestHandler(
        CreateWorkflowInstanceRequest.class,
        r -> CreateWorkflowInstanceResponse.getDefaultInstance());
    addRequestHandler(
        CancelWorkflowInstanceRequest.class,
        r -> CancelWorkflowInstanceResponse.getDefaultInstance());
    addRequestHandler(
        UpdateWorkflowInstancePayloadRequest.class,
        r -> UpdateWorkflowInstancePayloadResponse.getDefaultInstance());
    addRequestHandler(
        UpdateJobRetriesRequest.class, r -> UpdateJobRetriesResponse.getDefaultInstance());
    addRequestHandler(FailJobRequest.class, r -> FailJobResponse.getDefaultInstance());
    addRequestHandler(CompleteJobRequest.class, r -> CompleteJobResponse.getDefaultInstance());
  }

  @Override
  public void health(HealthRequest request, StreamObserver<HealthResponse> responseObserver) {
    handle(request, responseObserver);
  }

  @Override
  public void deployWorkflow(
      DeployWorkflowRequest request, StreamObserver<DeployWorkflowResponse> responseObserver) {
    handle(request, responseObserver);
  }

  @Override
  public void publishMessage(
      PublishMessageRequest request, StreamObserver<Empty> responseObserver) {
    handle(request, responseObserver);
  }

  @Override
  public void createJob(
      CreateJobRequest request, StreamObserver<CreateJobResponse> responseObserver) {
    handle(request, responseObserver);
  }

  @Override
  public void createWorkflowInstance(
      CreateWorkflowInstanceRequest request,
      StreamObserver<CreateWorkflowInstanceResponse> responseObserver) {
    handle(request, responseObserver);
  }

  @Override
  public void cancelWorkflowInstance(
      CancelWorkflowInstanceRequest request,
      StreamObserver<CancelWorkflowInstanceResponse> responseObserver) {
    handle(request, responseObserver);
  }

  @Override
  public void updateWorkflowInstancePayload(
      UpdateWorkflowInstancePayloadRequest request,
      StreamObserver<UpdateWorkflowInstancePayloadResponse> responseObserver) {
    handle(request, responseObserver);
  }

  @Override
  public void updateJobRetries(
      UpdateJobRetriesRequest request, StreamObserver<UpdateJobRetriesResponse> responseObserver) {
    handle(request, responseObserver);
  }

  @Override
  public void failJob(FailJobRequest request, StreamObserver<FailJobResponse> responseObserver) {
    handle(request, responseObserver);
  }

  @Override
  public void completeJob(
      CompleteJobRequest request, StreamObserver<CompleteJobResponse> responseObserver) {
    handle(request, responseObserver);
  }

  public static Partition partition(int partitionId, PartitionBrokerRole role) {
    return Partition.newBuilder().setPartitionId(partitionId).setRole(role).build();
  }

  public static BrokerInfo broker(String host, int port, Partition... partitions) {
    return BrokerInfo.newBuilder()
        .setHost(host)
        .setPort(port)
        .addAllPartitions(Arrays.asList(partitions))
        .build();
  }

  public void onHealthRequest(BrokerInfo... brokers) {
    addRequestHandler(
        HealthRequest.class,
        request -> HealthResponse.newBuilder().addAllBrokers(Arrays.asList(brokers)).build());
  }

  public static WorkflowResponseObject deployedWorkflow(
      String bpmnProcessId, int version, long workflowKey, String resourceName) {
    return WorkflowResponseObject.newBuilder()
        .setBpmnProcessId(bpmnProcessId)
        .setVersion(version)
        .setWorkflowKey(workflowKey)
        .setResourceName(resourceName)
        .build();
  }

  public void onDeployWorkflowRequest(WorkflowResponseObject... deployedWorkflows) {
    addRequestHandler(
        DeployWorkflowRequest.class,
        request ->
            DeployWorkflowResponse.newBuilder()
                .addAllWorkflows(Arrays.asList(deployedWorkflows))
                .build());
  }

  public void onCreateJobRequest(int partitionId, long key) {
    addRequestHandler(
        CreateJobRequest.class,
        request -> CreateJobResponse.newBuilder().setPartitionId(partitionId).setKey(key).build());
  }

  public void onCreateWorkflowInstanceRequest(
      long workflowKey,
      String bpmnProcessId,
      int version,
      int partitionId,
      long workflowInstanceKey) {
    addRequestHandler(
        CreateWorkflowInstanceRequest.class,
        request ->
            CreateWorkflowInstanceResponse.newBuilder()
                .setWorkflowKey(workflowKey)
                .setBpmnProcessId(bpmnProcessId)
                .setVersion(version)
                .setPartitionId(partitionId)
                .setWorkflowInstanceKey(workflowInstanceKey)
                .build());
  }

  public void errorOnRequest(
      Class<? extends GeneratedMessageV3> requestClass, Supplier<Exception> errorSupplier) {
    addRequestHandler(
        requestClass,
        request -> {
          throw errorSupplier.get();
        });
  }

  public List<GeneratedMessageV3> getRequests() {
    return requests;
  }

  @SuppressWarnings("unchecked")
  public <T extends GeneratedMessageV3> T getRequest(int index) {
    return (T) requests.get(index);
  }

  public <T extends GeneratedMessageV3> T getLastRequest() {
    return getRequest(requests.size() - 1);
  }

  public void addRequestHandler(
      Class<? extends GeneratedMessageV3> requestClass, RequestHandler requestHandler) {
    requestHandlers.put(requestClass, requestHandler);
  }

  @SuppressWarnings("unchecked")
  private <RequestT extends GeneratedMessageV3, ResponseT extends GeneratedMessageV3> void handle(
      RequestT request, StreamObserver<ResponseT> responseObserver) {
    requests.add(request);
    try {
      final ResponseT response = (ResponseT) getRequestHandler(request).handle(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(convertThrowable(e));
    }
  }

  private RequestHandler getRequestHandler(GeneratedMessageV3 request) {
    final RequestHandler requestHandler = requestHandlers.get(request.getClass());
    if (requestHandler == null) {
      throw new IllegalStateException(
          "No request handler found for request class: " + request.getClass());
    }

    return requestHandler;
  }

  private static StatusRuntimeException convertThrowable(final Throwable cause) {
    final String description;

    if (cause instanceof ExecutionException) {
      description = cause.getCause().getMessage();
    } else {
      description = cause.getMessage();
    }

    return Status.INTERNAL.augmentDescription(description).asRuntimeException();
  }

  @FunctionalInterface
  interface RequestHandler<
      RequestT extends GeneratedMessageV3, ResponseT extends GeneratedMessageV3> {
    ResponseT handle(RequestT request) throws Exception;
  }
}