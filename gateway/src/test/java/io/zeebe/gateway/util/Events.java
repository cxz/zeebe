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
package io.zeebe.gateway.util;

import static io.zeebe.gateway.util.ClientRule.DEFAULT_PARTITION;
import static io.zeebe.protocol.Protocol.DEFAULT_TOPIC;

import io.zeebe.gateway.api.events.JobEvent;
import io.zeebe.gateway.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.gateway.impl.event.JobActivateEventImpl;
import io.zeebe.gateway.impl.event.JobEventImpl;
import io.zeebe.gateway.impl.event.WorkflowInstanceEventImpl;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.util.Maps;

public class Events {

  public static JobActivateEventImpl exampleJobActivateResponse(
      String worker, String type, int amount, long timeout) {
    final ZeebeObjectMapperImpl mapper = new ZeebeObjectMapperImpl();
    final JobActivateEventImpl baseEvent = new JobActivateEventImpl(mapper);
    final List<JobEvent> jobs = new ArrayList<>();
    baseEvent.setJobs(jobs);

    for (int i = 0; i < amount; i++) {
      final JobEventImpl job = new JobEventImpl(mapper);
      job.setTopicName(DEFAULT_TOPIC);
      job.setPartitionId(DEFAULT_PARTITION);

      job.setIntent(JobIntent.ACTIVATED);
      job.setKey(i);

      job.setWorker(worker);
      job.setType(type);
      job.setPosition(i);
      jobs.add(job);
    }

    return baseEvent;
  }

  public static JobEventImpl exampleJob() {
    final JobEventImpl baseEvent = new JobEventImpl(new ZeebeObjectMapperImpl());
    baseEvent.setIntent(JobIntent.CREATED);
    baseEvent.setHeaders(Maps.newHashMap("defaultHeaderKey", "defaultHeaderVal"));
    baseEvent.setCustomHeaders(Maps.newHashMap("customHeaderKey", "customHeaderVal"));
    baseEvent.setKey(79);
    baseEvent.setWorker("foo");
    baseEvent.setDeadline(Instant.now());
    baseEvent.setPartitionId(DEFAULT_PARTITION);
    baseEvent.setPayload("{\"key\":\"val\"}");
    baseEvent.setRetries(123);
    baseEvent.setTopicName(DEFAULT_TOPIC);
    baseEvent.setType("taskTypeFoo");
    baseEvent.setPosition(456);

    return baseEvent;
  }

  public static WorkflowInstanceEventImpl exampleWorfklowInstance() {
    final WorkflowInstanceEventImpl baseEvent =
        new WorkflowInstanceEventImpl(new ZeebeObjectMapperImpl());
    baseEvent.setIntent(WorkflowInstanceIntent.CREATED);
    baseEvent.setActivityId("some_activity");
    baseEvent.setBpmnProcessId("some_proceess");
    baseEvent.setKey(89);
    baseEvent.setPayload("{\"key\":\"val\"}");
    baseEvent.setPartitionId(DEFAULT_PARTITION);
    baseEvent.setTopicName(DEFAULT_TOPIC);
    baseEvent.setVersion(123);
    baseEvent.setWorkflowInstanceKey(456L);
    baseEvent.setWorkflowKey(789L);
    baseEvent.setScopeInstanceKey(890L);

    return baseEvent;
  }
}
