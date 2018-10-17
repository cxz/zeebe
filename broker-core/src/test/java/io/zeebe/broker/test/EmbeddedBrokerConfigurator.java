/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.test;

import io.zeebe.broker.exporter.DebugExporter;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.broker.system.configuration.ExporterCfg;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.Arrays;
import java.util.function.Consumer;

public class EmbeddedBrokerConfigurator {

  public static final Consumer<BrokerCfg> NOOP = cfg -> {};

  public static final Consumer<BrokerCfg> DEBUG_EXPORTER =
      cfg -> cfg.getExporters().add(DebugExporter.defaultConfig(false));

  public static final Consumer<BrokerCfg> TEST_RECORDER =
      cfg -> {
        final ExporterCfg exporterCfg = new ExporterCfg();
        exporterCfg.setId("test-recorder");
        exporterCfg.setClassName(RecordingExporter.class.getName());
        cfg.getExporters().add(exporterCfg);
      };

  public static Consumer<BrokerCfg> setPartitionCount(final int partitionCount) {
    return cfg -> cfg.getCluster().setPartitionsCount(partitionCount);
  }

  public static Consumer<BrokerCfg> setCluster(
      final int nodeId,
      final int partitionCount,
      final int replicationFactor,
      final int clusterSize) {
    return cfg -> {
      final ClusterCfg cluster = cfg.getCluster();
      cluster.setNodeId(nodeId);
      cluster.setPartitionsCount(partitionCount);
      cluster.setReplicationFactor(replicationFactor);
      cluster.setClusterSize(clusterSize);
    };
  }

  public static Consumer<BrokerCfg> setInitialContactPoints(final String... contactPoints) {
    return cfg -> cfg.getCluster().setInitialContactPoints(Arrays.asList(contactPoints));
  }

  public static Consumer<BrokerCfg> setEmbedGateway(final boolean embedGateway) {
    return cfg -> cfg.setEmbedGateway(embedGateway);
  }

  public static Consumer<BrokerCfg> setClientApiPort(final int port) {
    return cfg -> cfg.getNetwork().getClient().setPort(port);
  }

  public static Consumer<BrokerCfg> setManagementApiPort(final int port) {
    return cfg -> cfg.getNetwork().getManagement().setPort(port);
  }

  public static Consumer<BrokerCfg> setReplicationApiPort(final int port) {
    return cfg -> cfg.getNetwork().getReplication().setPort(port);
  }

  public static Consumer<BrokerCfg> setSubscriptionApiPort(final int port) {
    return cfg -> cfg.getNetwork().getSubscription().setPort(port);
  }
}
