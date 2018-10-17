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

import static io.zeebe.gateway.configuration.EnvironmentConstants.ENV_CONTACT_POINT;

import io.zeebe.util.ByteValue;
import io.zeebe.util.DurationUtil;
import io.zeebe.util.Environment;
import java.time.Duration;

public class ClusterCfg {

  private String contactPoint = "127.0.0.1:26501";
  private String sendBufferSize = "2M";
  private String requestTimeout = "15s";

  public void init(GatewayCfg gatewayCfg, Environment environment) {
    environment.get(ENV_CONTACT_POINT).ifPresent(v -> contactPoint = v);
  }

  public String getContactPoint() {
    return contactPoint;
  }

  public ClusterCfg setContactPoint(String contactPoint) {
    this.contactPoint = contactPoint;
    return this;
  }

  public ByteValue getSendBufferSize() {
    return new ByteValue(sendBufferSize);
  }

  public ClusterCfg setSendBufferSize(String sendBufferSize) {
    this.sendBufferSize = sendBufferSize;
    return this;
  }

  public Duration getRequestTimeout() {
    return DurationUtil.parse(requestTimeout);
  }

  public ClusterCfg setRequestTimeout(String requestTimeout) {
    this.requestTimeout = requestTimeout;
    return this;
  }

  @Override
  public String toString() {
    return "ClusterCfg{"
        + "contactPoint='"
        + contactPoint
        + '\''
        + ", sendBufferSize='"
        + sendBufferSize
        + '\''
        + ", requestTimeout='"
        + requestTimeout
        + '\''
        + '}';
  }
}
