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
package io.zeebe.gateway.job.subscription;

import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.gateway.ZeebeClient;
import io.zeebe.gateway.api.subscription.JobWorker;
import io.zeebe.gateway.util.ClientRule;
import io.zeebe.protocol.clientapi.ControlMessageType;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.SubscriptionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.test.broker.protocol.brokerapi.ControlMessageRequest;
import io.zeebe.test.broker.protocol.brokerapi.StubBrokerRule;
import io.zeebe.test.broker.protocol.brokerapi.data.Topology;
import io.zeebe.transport.RemoteAddress;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class PartitionedJobSubscriptionTest {

  public static final int PARTITION_1 = 0;
  public static final int PARTITION_2 = 1;

  public static final String JOB_TYPE = "foo";

  public StubBrokerRule broker1 = new StubBrokerRule(0, 2, 2);
  public StubBrokerRule broker2 = new StubBrokerRule(1, 2, 2);
  public ClientRule clientRule = new ClientRule(broker1);

  protected ZeebeClient client;

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(broker1).around(broker2).around(clientRule);

  @Before
  public void setUp() {
    final Topology topology =
        new Topology().addLeader(broker1, PARTITION_1).addLeader(broker2, PARTITION_2);

    broker1.setCurrentTopology(topology);
    broker2.setCurrentTopology(topology);

    client = clientRule.getClient();
    // ensure update of topology
    client.newTopologyRequest().send().join();
  }

  @Test
  public void shouldSubscribeToMultiplePartitions() {
    // given
    broker1.stubJobSubscriptionApi(456);
    broker2.stubJobSubscriptionApi(789);

    // when
    final JobWorker subscription =
        client
            .jobClient()
            .newWorker()
            .jobType(JOB_TYPE)
            .handler(new RecordingJobHandler())
            .name("bumbum")
            .timeout(Duration.ofSeconds(6))
            .open();

    // then
    assertThat(subscription.isOpen()).isTrue();

    final List<ControlMessageRequest> subscribeRequestsBroker1 = getSubscribeRequests(broker1);
    assertThat(subscribeRequestsBroker1).hasSize(1);
    final ControlMessageRequest request1 = subscribeRequestsBroker1.get(0);
    assertThat(request1.partitionId()).isEqualTo(PARTITION_1);

    final List<ControlMessageRequest> subscribeRequestsBroker2 = getSubscribeRequests(broker2);
    assertThat(subscribeRequestsBroker2).hasSize(1);
    final ControlMessageRequest request2 = subscribeRequestsBroker2.get(0);
    assertThat(request2.partitionId()).isEqualTo(PARTITION_2);
  }

  protected List<ControlMessageRequest> getSubscribeRequests(final StubBrokerRule broker) {
    return broker
        .getReceivedControlMessageRequests()
        .stream()
        .filter(r -> r.messageType() == ControlMessageType.ADD_JOB_SUBSCRIPTION)
        .collect(Collectors.toList());
  }

  @Test
  public void shouldReceiveEventsFromMultiplePartitions() {
    // given
    final int subscriberKey1 = 456;
    broker1.stubJobSubscriptionApi(subscriberKey1);

    final int subscriberKey2 = 789;
    broker2.stubJobSubscriptionApi(subscriberKey2);

    final RecordingJobHandler eventHandler = new RecordingJobHandler();
    client
        .jobClient()
        .newWorker()
        .jobType(JOB_TYPE)
        .handler(eventHandler)
        .name("bumbum")
        .timeout(Duration.ofSeconds(6))
        .open();

    final RemoteAddress clientAddressFromBroker1 =
        broker1
            .getReceivedControlMessageRequestsByType(ControlMessageType.ADD_JOB_SUBSCRIPTION)
            .get(0)
            .getSource();
    final RemoteAddress clientAddressFromBroker2 =
        broker2
            .getReceivedControlMessageRequestsByType(ControlMessageType.ADD_JOB_SUBSCRIPTION)
            .get(0)
            .getSource();

    // when
    final long key1 = 3;
    broker1
        .newSubscribedEvent()
        .partitionId(PARTITION_1)
        .valueType(ValueType.JOB)
        .recordType(RecordType.EVENT)
        .intent(JobIntent.ACTIVATED)
        .subscriberKey(subscriberKey1)
        .key(key1)
        .subscriptionType(SubscriptionType.JOB_SUBSCRIPTION)
        .value()
        .done()
        .push(clientAddressFromBroker1);

    final long key2 = 4;
    broker2
        .newSubscribedEvent()
        .partitionId(PARTITION_1)
        .valueType(ValueType.JOB)
        .recordType(RecordType.EVENT)
        .intent(JobIntent.ACTIVATED)
        .subscriberKey(subscriberKey1)
        .key(key2)
        .subscriptionType(SubscriptionType.JOB_SUBSCRIPTION)
        .value()
        .done()
        .push(clientAddressFromBroker2);

    // then
    waitUntil(() -> eventHandler.numHandledJobs() == 2);
    assertThat(eventHandler.getHandledJobs())
        .extracting("metadata.key")
        .containsExactlyInAnyOrder(key1, key2);
  }
}
