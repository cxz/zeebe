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
package io.zeebe.gateway.impl.workflow;

import io.zeebe.gateway.api.commands.UpdatePayloadWorkflowInstanceCommandStep1;
import io.zeebe.gateway.api.commands.UpdatePayloadWorkflowInstanceCommandStep1.UpdatePayloadWorkflowInstanceCommandStep2;
import io.zeebe.gateway.api.events.WorkflowInstanceEvent;
import io.zeebe.gateway.impl.CommandImpl;
import io.zeebe.gateway.impl.RequestManager;
import io.zeebe.gateway.impl.command.WorkflowInstanceCommandImpl;
import io.zeebe.gateway.impl.event.WorkflowInstanceEventImpl;
import io.zeebe.gateway.impl.record.RecordImpl;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.util.EnsureUtil;
import java.io.InputStream;
import java.util.Map;

public class UpdatePayloadCommandImpl extends CommandImpl<WorkflowInstanceEvent>
    implements UpdatePayloadWorkflowInstanceCommandStep1,
        UpdatePayloadWorkflowInstanceCommandStep2 {
  private final WorkflowInstanceCommandImpl command;

  public UpdatePayloadCommandImpl(
      final RequestManager commandManager, WorkflowInstanceEvent event) {
    super(commandManager);

    EnsureUtil.ensureNotNull("base event", event);

    command =
        new WorkflowInstanceCommandImpl(
            (WorkflowInstanceEventImpl) event, WorkflowInstanceIntent.UPDATE_PAYLOAD);
  }

  @Override
  public UpdatePayloadWorkflowInstanceCommandStep2 payload(InputStream payload) {
    command.setPayload(payload);
    return this;
  }

  @Override
  public UpdatePayloadWorkflowInstanceCommandStep2 payload(String payload) {
    command.setPayload(payload);
    return this;
  }

  @Override
  public UpdatePayloadWorkflowInstanceCommandStep2 payload(Map<String, Object> payload) {
    command.setPayload(payload);
    return this;
  }

  @Override
  public UpdatePayloadWorkflowInstanceCommandStep2 payload(Object payload) {
    command.setPayload(payload);
    return this;
  }

  @Override
  public RecordImpl getCommand() {
    return command;
  }
}
