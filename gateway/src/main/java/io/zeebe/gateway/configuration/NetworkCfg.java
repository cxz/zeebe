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
package io.zeebe.gateway.configuration;

import static io.zeebe.gateway.configuration.EnvironmentConstants.ENV_HOST;
import static io.zeebe.gateway.configuration.EnvironmentConstants.ENV_PORT;

import io.zeebe.util.Environment;
import java.net.InetSocketAddress;

public class NetworkCfg {

  private String host = "0.0.0.0";
  private int port = 26500;

  public void init(GatewayCfg gatewayCfg, Environment environment) {
    environment.get(ENV_HOST).ifPresent(v -> host = v);
    environment.getInt(ENV_PORT).ifPresent(v -> port = v);
  }

  public String getHost() {
    return host;
  }

  public NetworkCfg setHost(String host) {
    this.host = host;
    return this;
  }

  public int getPort() {
    return port;
  }

  public NetworkCfg setPort(int port) {
    this.port = port;
    return this;
  }

  public InetSocketAddress toInetAddress() {
    return new InetSocketAddress(host, port);
  }

  @Override
  public String toString() {
    return "NetworkCfg{" + "host='" + host + '\'' + ", port=" + port + '}';
  }
}
