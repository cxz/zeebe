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
package io.zeebe.protocol.intent;

import io.zeebe.protocol.clientapi.ValueType;
import java.util.Arrays;
import java.util.Collection;

public interface Intent {
  Collection<Class<? extends Intent>> INTENT_CLASSES =
      Arrays.asList(
          DeploymentIntent.class,
          IdIntent.class,
          IncidentIntent.class,
          SubscriberIntent.class,
          SubscriptionIntent.class,
          JobIntent.class,
          TopicIntent.class,
          WorkflowInstanceIntent.class,
          MessageIntent.class,
          MessageSubscriptionIntent.class,
          WorkflowInstanceSubscriptionIntent.class);

  Intent UNKNOWN =
      new Intent() {
        @Override
        public short value() {
          return NULL_VAL;
        }

        @Override
        public String name() {
          return "UNKNOWN";
        }
      };

  short NULL_VAL = 255;

  short value();

  String name();

  static Intent fromProtocolValue(ValueType valueType, short intent) {
    switch (valueType) {
      case DEPLOYMENT:
        return DeploymentIntent.from(intent);
      case ID:
        return IdIntent.from(intent);
      case INCIDENT:
        return IncidentIntent.from(intent);
      case NOOP:
        return Intent.UNKNOWN;
      case RAFT:
        return RaftIntent.from(intent);
      case SUBSCRIBER:
        return SubscriberIntent.from(intent);
      case SUBSCRIPTION:
        return SubscriptionIntent.from(intent);
      case JOB:
        return JobIntent.from(intent);
      case TOPIC:
        return TopicIntent.from(intent);
      case WORKFLOW_INSTANCE:
        return WorkflowInstanceIntent.from(intent);
      case MESSAGE:
        return MessageIntent.from(intent);
      case MESSAGE_SUBSCRIPTION:
        return MessageSubscriptionIntent.from(intent);
      case WORKFLOW_INSTANCE_SUBSCRIPTION:
        return WorkflowInstanceSubscriptionIntent.from(intent);
      case EXPORTER:
        return ExporterIntent.from(intent);
      case NULL_VAL:
      case SBE_UNKNOWN:
        return Intent.UNKNOWN;
      default:
        throw new RuntimeException("unknown type");
    }
  }

  static Intent fromProtocolValue(ValueType valueType, String intent) {
    switch (valueType) {
      case DEPLOYMENT:
        return DeploymentIntent.valueOf(intent);
      case ID:
        return IdIntent.valueOf(intent);
      case INCIDENT:
        return IncidentIntent.valueOf(intent);
      case NOOP:
        return Intent.UNKNOWN;
      case RAFT:
        return RaftIntent.valueOf(intent);
      case SUBSCRIBER:
        return SubscriberIntent.valueOf(intent);
      case SUBSCRIPTION:
        return SubscriptionIntent.valueOf(intent);
      case JOB:
        return JobIntent.valueOf(intent);
      case TOPIC:
        return TopicIntent.valueOf(intent);
      case WORKFLOW_INSTANCE:
        return WorkflowInstanceIntent.valueOf(intent);
      case MESSAGE:
        return WorkflowInstanceIntent.valueOf(intent);
      case MESSAGE_SUBSCRIPTION:
        return MessageSubscriptionIntent.valueOf(intent);
      case WORKFLOW_INSTANCE_SUBSCRIPTION:
        return WorkflowInstanceSubscriptionIntent.valueOf(intent);
      case NULL_VAL:
      case SBE_UNKNOWN:
        return Intent.UNKNOWN;
      default:
        throw new RuntimeException("unknown type");
    }
  }

  static int maxCardinality() {
    return INTENT_CLASSES
        .stream()
        .mapToInt(clazz -> clazz.getEnumConstants().length)
        .max()
        .getAsInt();
  }
}
