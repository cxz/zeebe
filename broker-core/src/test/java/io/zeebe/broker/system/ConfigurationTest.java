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
package io.zeebe.broker.system;

import static io.zeebe.broker.system.configuration.ClusterCfg.DEFAULT_CLUSTER_SIZE;
import static io.zeebe.broker.system.configuration.ClusterCfg.DEFAULT_CONTACT_POINTS;
import static io.zeebe.broker.system.configuration.ClusterCfg.DEFAULT_NODE_ID;
import static io.zeebe.broker.system.configuration.ClusterCfg.DEFAULT_PARTITIONS_COUNT;
import static io.zeebe.broker.system.configuration.ClusterCfg.DEFAULT_REPLICATION_FACTOR;
import static io.zeebe.broker.system.configuration.DataCfg.DEFAULT_DIRECTORY;
import static io.zeebe.broker.system.configuration.EnvironmentConstants.ENV_CLUSTER_SIZE;
import static io.zeebe.broker.system.configuration.EnvironmentConstants.ENV_DIRECTORIES;
import static io.zeebe.broker.system.configuration.EnvironmentConstants.ENV_HOST;
import static io.zeebe.broker.system.configuration.EnvironmentConstants.ENV_INITIAL_CONTACT_POINTS;
import static io.zeebe.broker.system.configuration.EnvironmentConstants.ENV_NODE_ID;
import static io.zeebe.broker.system.configuration.EnvironmentConstants.ENV_PARTITIONS_COUNT;
import static io.zeebe.broker.system.configuration.EnvironmentConstants.ENV_PORT_OFFSET;
import static io.zeebe.broker.system.configuration.EnvironmentConstants.ENV_REPLICATION_FACTOR;
import static io.zeebe.broker.system.configuration.NetworkCfg.DEFAULT_HOST;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.broker.system.configuration.DataCfg;
import io.zeebe.broker.system.configuration.ExporterCfg;
import io.zeebe.broker.system.configuration.NetworkCfg;
import io.zeebe.broker.system.configuration.SocketBindingClientApiCfg;
import io.zeebe.broker.system.configuration.SocketBindingManagementCfg;
import io.zeebe.broker.system.configuration.SocketBindingReplicationCfg;
import io.zeebe.broker.system.configuration.SocketBindingSubscriptionCfg;
import io.zeebe.util.Environment;
import io.zeebe.util.TomlConfigurationReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.assertj.core.api.Condition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ConfigurationTest {

  public static final String BROKER_BASE = "test";
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public Map<String, String> environment = new HashMap<>();

  public static final int CLIENT_PORT = SocketBindingClientApiCfg.DEFAULT_PORT;
  public static final int MANAGEMENT_PORT = SocketBindingManagementCfg.DEFAULT_PORT;
  public static final int REPLICATION_PORT = SocketBindingReplicationCfg.DEFAULT_PORT;
  public static final int SUBSCRIPTION_PORT = SocketBindingSubscriptionCfg.DEFAULT_PORT;

  @Test
  public void shouldUseSpecifiedNodeId() {
    assertNodeId("specific-node-id", 123);
  }

  @Test
  public void shouldUseNodeIdFromEnvironment() {
    environment.put(ENV_NODE_ID, "42");
    assertNodeId("default", 42);
  }

  @Test
  public void shouldUseNodeIdFromEnvironmentWithSpecifiedNodeId() {
    environment.put(ENV_NODE_ID, "42");
    assertNodeId("specific-node-id", 42);
  }

  @Test
  public void shouldIgnoreInvalidNodeIdFromEnvironment() {
    environment.put(ENV_NODE_ID, "a");
    assertNodeId("default", DEFAULT_NODE_ID);
  }

  @Test
  public void shouldUseDefaultPorts() {
    assertPorts("default", CLIENT_PORT, MANAGEMENT_PORT, REPLICATION_PORT, SUBSCRIPTION_PORT);
  }

  @Test
  public void shouldUseSpecifiedPorts() {
    assertPorts("specific-ports", 1, 2, 3, 4);
  }

  @Test
  public void shouldUsePortOffset() {
    final int offset = 50;
    assertPorts(
        "port-offset",
        CLIENT_PORT + offset,
        MANAGEMENT_PORT + offset,
        REPLICATION_PORT + offset,
        SUBSCRIPTION_PORT + offset);
  }

  @Test
  public void shouldUsePortOffsetWithSpecifiedPorts() {
    final int offset = 30;
    assertPorts("specific-ports-offset", 1 + offset, 2 + offset, 3 + offset, 4 + offset);
  }

  @Test
  public void shouldUsePortOffsetFromEnvironment() {
    environment.put(ENV_PORT_OFFSET, "5");
    final int offset = 50;
    assertPorts(
        "default",
        CLIENT_PORT + offset,
        MANAGEMENT_PORT + offset,
        REPLICATION_PORT + offset,
        SUBSCRIPTION_PORT + offset);
  }

  @Test
  public void shouldUsePortOffsetFromEnvironmentWithSpecifiedPorts() {
    environment.put(ENV_PORT_OFFSET, "3");
    final int offset = 30;
    assertPorts("specific-ports", 1 + offset, 2 + offset, 3 + offset, 4 + offset);
  }

  @Test
  public void shouldIgnoreInvalidPortOffsetFromEnvironment() {
    environment.put(ENV_PORT_OFFSET, "a");
    assertPorts("default", CLIENT_PORT, MANAGEMENT_PORT, REPLICATION_PORT, SUBSCRIPTION_PORT);
  }

  @Test
  public void shouldOverridePortOffsetFromEnvironment() {
    environment.put(ENV_PORT_OFFSET, "7");
    final int offset = 70;
    assertPorts(
        "port-offset",
        CLIENT_PORT + offset,
        MANAGEMENT_PORT + offset,
        REPLICATION_PORT + offset,
        SUBSCRIPTION_PORT + offset);
  }

  @Test
  public void shouldExpandExporterJarPathRelativeToBrokerBaseIffPresent() {
    // given
    final InputStream input =
        new ByteArrayInputStream(
            ("[[exporters]]\n"
                    + "id=\"external\"\n"
                    + "jarPath=\"exporters/exporter.jar\"\n"
                    + "[[exporters]]\n"
                    + "id=\"internal-1\"\n"
                    + "jarPath=\"\"\n"
                    + "[[exporters]]\n"
                    + "id=\"internal-2\"")
                .getBytes());
    final BrokerCfg config = TomlConfigurationReader.read(input, BrokerCfg.class);
    final String base = temporaryFolder.getRoot().getAbsolutePath();
    final String jarFile = Paths.get(base, "exporters", "exporter.jar").toAbsolutePath().toString();

    // when
    config.init(base);

    // then
    assertThat(config.getExporters()).hasSize(3);
    assertThat(config.getExporters().get(0))
        .hasFieldOrPropertyWithValue("jarPath", jarFile)
        .is(new Condition<>(ExporterCfg::isExternal, "is external"));
    assertThat(config.getExporters().get(1).isExternal()).isFalse();
    assertThat(config.getExporters().get(2).isExternal()).isFalse();
  }

  @Test
  public void shouldUseDefaultHost() {
    assertHost("default", DEFAULT_HOST);
  }

  @Test
  public void shouldUseSpecifiedHosts() {
    assertHost(
        "specific-hosts",
        DEFAULT_HOST,
        "clientHost",
        "managementHost",
        "replicationHost",
        "subscriptionHost");
  }

  @Test
  public void shouldUseGlobalHost() {
    assertHost("host", "1.1.1.1");
  }

  @Test
  public void shouldUseHostFromEnvironment() {
    environment.put(ENV_HOST, "2.2.2.2");
    assertHost("default", "2.2.2.2");
  }

  @Test
  public void shouldUseHostFromEnvironmentWithGlobalHost() {
    environment.put(ENV_HOST, "myHost");
    assertHost("host", "myHost");
  }

  @Test
  public void shouldNotOverrideSpecifiedHostsFromEnvironment() {
    environment.put(ENV_HOST, "myHost");
    assertHost(
        "specific-hosts",
        "myHost",
        "clientHost",
        "managementHost",
        "replicationHost",
        "subscriptionHost");
  }

  @Test
  public void shouldUseDefaultContactPoints() {
    assertContactPoints("default", DEFAULT_CONTACT_POINTS);
  }

  @Test
  public void shouldUseSpecifiedContactPoints() {
    assertContactPoints("contact-points", "broker1", "broker2", "broker3");
  }

  @Test
  public void shouldUseContactPointsFromEnvironment() {
    environment.put(ENV_INITIAL_CONTACT_POINTS, "foo,bar");
    assertContactPoints("default", "foo", "bar");
  }

  @Test
  public void shouldUseContactPointsFromEnvironmentWithSpecifiedContactPoints() {
    environment.put(ENV_INITIAL_CONTACT_POINTS, "1.1.1.1,2.2.2.2");
    assertContactPoints("contact-points", "1.1.1.1", "2.2.2.2");
  }

  @Test
  public void shouldUseSingleContactPointFromEnvironment() {
    environment.put(ENV_INITIAL_CONTACT_POINTS, "hello");
    assertContactPoints("contact-points", "hello");
  }

  @Test
  public void shouldClearContactPointFromEnvironment() {
    environment.put(ENV_INITIAL_CONTACT_POINTS, "");
    assertContactPoints("contact-points");
  }

  @Test
  public void shouldIgnoreTrailingCommaContactPointFromEnvironment() {
    environment.put(ENV_INITIAL_CONTACT_POINTS, "foo,bar,");
    assertContactPoints("contact-points", "foo", "bar");
  }

  @Test
  public void shouldUseDefaultDirectories() {
    assertDirectories("default", DEFAULT_DIRECTORY);
  }

  @Test
  public void shouldUseSpecifiedDirectories() {
    assertDirectories("directories", "data1", "data2", "data3");
  }

  @Test
  public void shouldUseDirectoriesFromEnvironment() {
    environment.put(ENV_DIRECTORIES, "foo,bar");
    assertDirectories("default", "foo", "bar");
  }

  @Test
  public void shouldUseDirectoriesFromEnvironmentWithSpecifiedDirectories() {
    environment.put(ENV_DIRECTORIES, "foo,bar");
    assertDirectories("directories", "foo", "bar");
  }

  @Test
  public void shouldUseSingleDirectoryFromEnvironment() {
    environment.put(ENV_DIRECTORIES, "hello");
    assertDirectories("directories", "hello");
  }

  @Test
  public void shouldIgnoreTrailingCommaDirectoriesFromEnvironment() {
    environment.put(ENV_DIRECTORIES, "foo,bar,");
    assertDirectories("directories", "foo", "bar");
  }

  @Test
  public void shouldReadDefaultSystemClusterConfiguration() {
    // given
    final BrokerCfg cfg = readConfig("default");
    final ClusterCfg cfgCluster = cfg.getCluster();

    // when - then
    assertThat(cfgCluster.getInitialContactPoints()).isEmpty();
    assertThat(cfgCluster.getNodeId()).isEqualTo(DEFAULT_NODE_ID);
    assertThat(cfgCluster.getPartitionsCount()).isEqualTo(DEFAULT_PARTITIONS_COUNT);
    assertThat(cfgCluster.getReplicationFactor()).isEqualTo(DEFAULT_REPLICATION_FACTOR);
    assertThat(cfgCluster.getClusterSize()).isEqualTo(DEFAULT_CLUSTER_SIZE);
  }

  @Test
  public void shouldReadSpecificSystemClusterConfiguration() {
    // given
    final BrokerCfg cfg = readConfig("cluster-cfg");
    final ClusterCfg cfgCluster = cfg.getCluster();

    // when - then
    assertThat(cfgCluster.getInitialContactPoints()).isEmpty();
    assertThat(cfgCluster.getNodeId()).isEqualTo(2);
    assertThat(cfgCluster.getPartitionsCount()).isEqualTo(3);
    assertThat(cfgCluster.getReplicationFactor()).isEqualTo(4);
    assertThat(cfgCluster.getClusterSize()).isEqualTo(5);
  }

  @Test
  public void shouldCreatePartitionIds() {
    // given
    final BrokerCfg cfg = readConfig("cluster-cfg");
    final ClusterCfg cfgCluster = cfg.getCluster();

    // when - then
    assertThat(cfgCluster.getPartitionsCount()).isEqualTo(3);
    final List<Integer> partitionIds = cfgCluster.getPartitionIds();
    assertThat(partitionIds).contains(0, 1, 2);
  }

  @Test
  public void shouldOverrideReplicationFactorViaEnvironment() {
    // given
    environment.put(ENV_REPLICATION_FACTOR, "2");

    // when
    final BrokerCfg cfg = readConfig("cluster-cfg");
    final ClusterCfg cfgCluster = cfg.getCluster();

    // then
    assertThat(cfgCluster.getReplicationFactor()).isEqualTo(2);
  }

  @Test
  public void shouldOverridePartitionsCountViaEnvironment() {
    // given
    environment.put(ENV_PARTITIONS_COUNT, "2");

    // when
    final BrokerCfg cfg = readConfig("cluster-cfg");
    final ClusterCfg cfgCluster = cfg.getCluster();

    // then
    assertThat(cfgCluster.getPartitionsCount()).isEqualTo(2);
  }

  @Test
  public void shouldOverrideClusterSizeViaEnvironment() {
    // given
    environment.put(ENV_CLUSTER_SIZE, "2");

    // when
    final BrokerCfg cfg = readConfig("cluster-cfg");
    final ClusterCfg cfgCluster = cfg.getCluster();

    // then
    assertThat(cfgCluster.getClusterSize()).isEqualTo(2);
  }

  @Test
  public void shouldOverrideAllClusterPropertiesViaEnvironment() {
    // given
    environment.put(ENV_CLUSTER_SIZE, "1");
    environment.put(ENV_PARTITIONS_COUNT, "2");
    environment.put(ENV_REPLICATION_FACTOR, "3");
    environment.put(ENV_NODE_ID, "4");

    // when
    final BrokerCfg cfg = readConfig("cluster-cfg");
    final ClusterCfg cfgCluster = cfg.getCluster();

    // then
    assertThat(cfgCluster.getClusterSize()).isEqualTo(1);
    assertThat(cfgCluster.getPartitionsCount()).isEqualTo(2);
    assertThat(cfgCluster.getReplicationFactor()).isEqualTo(3);
    assertThat(cfgCluster.getNodeId()).isEqualTo(4);
  }

  private BrokerCfg readConfig(final String name) {
    final String configPath = "/system/" + name + ".toml";
    final InputStream resourceAsStream = ConfigurationTest.class.getResourceAsStream(configPath);
    assertThat(resourceAsStream)
        .withFailMessage("Unable to read configuration file %s", configPath)
        .isNotNull();

    final BrokerCfg config = TomlConfigurationReader.read(resourceAsStream, BrokerCfg.class);
    config.init(BROKER_BASE, new Environment(environment));
    return config;
  }

  private void assertNodeId(final String configFileName, final int nodeId) {
    final BrokerCfg cfg = readConfig(configFileName);
    assertThat(cfg.getCluster().getNodeId()).isEqualTo(nodeId);
  }

  private void assertPorts(
      final String configFileName,
      final int client,
      final int management,
      final int replication,
      final int subscription) {
    final NetworkCfg network = readConfig(configFileName).getNetwork();
    assertThat(network.getClient().getPort()).isEqualTo(client);
    assertThat(network.getManagement().getPort()).isEqualTo(management);
    assertThat(network.getReplication().getPort()).isEqualTo(replication);
    assertThat(network.getSubscription().getPort()).isEqualTo(subscription);
  }

  private void assertHost(final String configFileName, final String host) {
    assertHost(configFileName, host, host, host, host, host);
  }

  private void assertHost(
      final String configFileName,
      final String host,
      final String client,
      final String management,
      final String replication,
      final String subscription) {
    final NetworkCfg cfg = readConfig(configFileName).getNetwork();
    assertThat(cfg.getHost()).isEqualTo(host);
    assertThat(cfg.getClient().getHost()).isEqualTo(client);
    assertThat(cfg.getManagement().getHost()).isEqualTo(management);
    assertThat(cfg.getReplication().getHost()).isEqualTo(replication);
    assertThat(cfg.getSubscription().getHost()).isEqualTo(subscription);
  }

  private void assertContactPoints(final String configFileName, final String... contactPoints) {
    assertContactPoints(configFileName, Arrays.asList(contactPoints));
  }

  private void assertContactPoints(final String configFileName, final List<String> contactPoints) {
    final ClusterCfg cfg = readConfig(configFileName).getCluster();
    assertThat(cfg.getInitialContactPoints()).containsExactlyElementsOf(contactPoints);
  }

  private void assertDirectories(final String configFileName, final String... directories) {
    assertDirectories(configFileName, Arrays.asList(directories));
  }

  private void assertDirectories(final String configFileName, final List<String> directories) {
    final DataCfg cfg = readConfig(configFileName).getData();
    final List<String> expected =
        directories
            .stream()
            .map(d -> Paths.get(BROKER_BASE, d).toString())
            .collect(Collectors.toList());
    assertThat(cfg.getDirectories()).containsExactlyElementsOf(expected);
  }
}
