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
package io.zeebe.gateway;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.zeebe.gateway.configuration.ClusterCfg;
import io.zeebe.gateway.configuration.GatewayCfg;
import io.zeebe.gateway.impl.ZeebeClientBuilderImpl;
import io.zeebe.gateway.impl.broker.BrokerClient;
import java.io.IOException;
import java.util.function.Function;
import org.slf4j.Logger;

public class Gateway {

  private static final Logger LOG = Loggers.GATEWAY_LOGGER;

  private static final String VERSION;

  static {
    final String version = Gateway.class.getPackage().getImplementationVersion();
    VERSION = version != null ? version : "development";
  }

  private final GatewayCfg gatewayCfg;
  private final Function<GatewayCfg, ServerBuilder> serverBuilderFactory;

  private Server server;
  private BrokerClient brokerClient;

  public Gateway() {
    this(new GatewayCfg());
  }

  public Gateway(GatewayCfg gatewayCfg) {
    this(gatewayCfg, cfg -> NettyServerBuilder.forAddress(cfg.getNetwork().toInetAddress()));
  }

  public Gateway(GatewayCfg gatewayCfg, Function<GatewayCfg, ServerBuilder> serverBuilderFactory) {
    this.gatewayCfg = gatewayCfg;
    this.serverBuilderFactory = serverBuilderFactory;
  }

  public void start() throws IOException {
    LOG.info("Version: {}", VERSION);
    LOG.info("Starting gateway with configuration {}", gatewayCfg.toJson());

    brokerClient = buildBrokerClient();

    server =
        serverBuilderFactory
            .apply(gatewayCfg)
            .addService(new EndpointManager(brokerClient))
            .build();

    server.start();
  }

  protected BrokerClient buildBrokerClient() {
    final ZeebeClientBuilderImpl brokerClientBuilder = new ZeebeClientBuilderImpl();

    final ClusterCfg clusterCfg = gatewayCfg.getCluster();
    brokerClientBuilder
        .brokerContactPoint(clusterCfg.getContactPoint())
        .requestTimeout(clusterCfg.getRequestTimeout())
        .sendBufferSize((int) clusterCfg.getSendBufferSize().toMegabytesValue().getValue())
        .numManagementThreads(gatewayCfg.getThreads().getManagementThreads());

    return brokerClientBuilder.buildBrokerClient();
  }

  public void listenAndServe() throws InterruptedException, IOException {
    start();
    server.awaitTermination();
  }

  public void stop() {
    if (brokerClient != null) {
      brokerClient.close();
      brokerClient = null;
    }

    if (server != null && !server.isShutdown()) {
      server.shutdown();
      try {
        server.awaitTermination();
      } catch (InterruptedException e) {
        LOG.error("Failed to await termination of gateway", e);
      } finally {
        server = null;
      }
    }
  }
}
