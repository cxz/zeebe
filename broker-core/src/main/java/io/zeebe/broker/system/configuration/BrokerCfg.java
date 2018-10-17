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
package io.zeebe.broker.system.configuration;

import static io.zeebe.broker.system.configuration.EnvironmentConstants.ENV_EMBED_GATEWAY;

import com.google.gson.GsonBuilder;
import io.zeebe.gossip.GossipConfiguration;
import io.zeebe.raft.RaftConfiguration;
import io.zeebe.util.Environment;
import java.util.ArrayList;
import java.util.List;

public class BrokerCfg {

  private boolean embedGateway = true;

  private NetworkCfg network = new NetworkCfg();
  private ClusterCfg cluster = new ClusterCfg();
  private ThreadsCfg threads = new ThreadsCfg();
  private MetricsCfg metrics = new MetricsCfg();
  private DataCfg data = new DataCfg();
  private GossipConfiguration gossip = new GossipConfiguration();
  private RaftConfiguration raft = new RaftConfiguration();
  private List<ExporterCfg> exporters = new ArrayList<>();

  public void init(final String brokerBase) {
    init(brokerBase, new Environment());
  }

  public void init(final String brokerBase, final Environment environment) {
    applyEnvironment(environment);
    network.init(this, brokerBase, environment);
    cluster.init(this, brokerBase, environment);
    threads.init(this, brokerBase, environment);
    metrics.init(this, brokerBase, environment);
    data.init(this, brokerBase, environment);
    exporters.forEach(e -> e.init(this, brokerBase, environment));
  }

  private void applyEnvironment(final Environment environment) {
    environment.getBool(ENV_EMBED_GATEWAY).ifPresent(v -> embedGateway = v);
  }

  public boolean isEmbedGateway() {
    return embedGateway;
  }

  public BrokerCfg setEmbedGateway(boolean embedGateway) {
    this.embedGateway = embedGateway;
    return this;
  }

  public NetworkCfg getNetwork() {
    return network;
  }

  public void setNetwork(final NetworkCfg network) {
    this.network = network;
  }

  public ClusterCfg getCluster() {
    return cluster;
  }

  public void setCluster(final ClusterCfg cluster) {
    this.cluster = cluster;
  }

  public ThreadsCfg getThreads() {
    return threads;
  }

  public void setThreads(final ThreadsCfg threads) {
    this.threads = threads;
  }

  public MetricsCfg getMetrics() {
    return metrics;
  }

  public void setMetrics(final MetricsCfg metrics) {
    this.metrics = metrics;
  }

  public DataCfg getData() {
    return data;
  }

  public void setData(final DataCfg logs) {
    this.data = logs;
  }

  public GossipConfiguration getGossip() {
    return gossip;
  }

  public void setGossip(final GossipConfiguration gossip) {
    this.gossip = gossip;
  }

  public RaftConfiguration getRaft() {
    return raft;
  }

  public void setRaft(final RaftConfiguration raft) {
    this.raft = raft;
  }

  public List<ExporterCfg> getExporters() {
    return exporters;
  }

  public void setExporters(final List<ExporterCfg> exporters) {
    this.exporters = exporters;
  }

  @Override
  public String toString() {
    return "BrokerCfg{"
        + "network="
        + network
        + ", cluster="
        + cluster
        + ", threads="
        + threads
        + ", metrics="
        + metrics
        + ", data="
        + data
        + ", gossip="
        + gossip
        + ", raft="
        + raft
        + ", exporters="
        + exporters
        + '}';
  }

  public String toJson() {
    return new GsonBuilder().setPrettyPrinting().create().toJson(this);
  }
}
