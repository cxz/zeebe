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
package io.zeebe.client.impl.event;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.zeebe.client.api.events.MessageEvent;
import io.zeebe.client.api.events.MessageState;
import io.zeebe.client.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.client.impl.record.MessageRecordImpl;
import io.zeebe.protocol.clientapi.RecordType;

public class MessageEventImpl extends MessageRecordImpl implements MessageEvent {

  public MessageEventImpl(@JacksonInject ZeebeObjectMapperImpl objectMapper) {
    super(objectMapper, RecordType.EVENT);
  }

  @JsonIgnore
  @Override
  public MessageState getState() {
    return MessageState.valueOf(getMetadata().getIntent());
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("Message [state=");
    builder.append(getState());
    builder.append(", messageName=");
    builder.append(getMessageName());
    builder.append(", messageKey=");
    builder.append(getMessageKey());
    builder.append(", payload=");
    builder.append(getPayload());
    builder.append("]");
    return builder.toString();
  }
}