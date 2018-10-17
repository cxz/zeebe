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

import static java.lang.Runtime.getRuntime;

import io.zeebe.gateway.configuration.GatewayCfg;
import io.zeebe.util.TomlConfigurationReader;
import java.nio.file.Paths;

public class StandaloneGateway {

  public static void main(final String[] args) throws Exception {
    final GatewayCfg config;

    if (args.length == 1) {
      config = readGatewayFromConfiguration(args);
    } else {
      config = new GatewayCfg();
    }

    final Gateway gateway = new Gateway(config);

    getRuntime()
        .addShutdownHook(
            new Thread("Broker close Thread") {
              @Override
              public void run() {
                gateway.stop();
              }
            });

    gateway.listenAndServe();
  }

  private static GatewayCfg readGatewayFromConfiguration(final String[] args) {
    String basePath = System.getProperty("basedir");

    if (basePath == null) {
      basePath = Paths.get(".").toAbsolutePath().normalize().toString();
    }

    String configFileLocation = args[0];
    if (!Paths.get(configFileLocation).isAbsolute()) {
      configFileLocation =
          Paths.get(basePath, configFileLocation).normalize().toAbsolutePath().toString();
    }

    return TomlConfigurationReader.read(configFileLocation, GatewayCfg.class);
  }
}
