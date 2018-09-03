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
package io.zeebe.broker.workflow.processor.deployment;

import io.zeebe.broker.logstreams.processor.TypedBatchWriter;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.subscription.message.data.MessageSubscriptionRecord;
import io.zeebe.broker.workflow.deployment.data.DeploymentRecord;
import io.zeebe.broker.workflow.deployment.data.Workflow;
import io.zeebe.broker.workflow.map.DeployedWorkflow;
import io.zeebe.broker.workflow.map.WorkflowCache;
import io.zeebe.broker.workflow.model.ExecutableStartEvent;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import org.agrona.DirectBuffer;

public class MessageStartEventDeploymentProcessor
    implements TypedRecordProcessor<DeploymentRecord> {

  private WorkflowCache workflowCache;

  public MessageStartEventDeploymentProcessor(final WorkflowCache workflowCache) {
    this.workflowCache = workflowCache;
  }

  @Override
  public void processRecord(
      TypedRecord<DeploymentRecord> record,
      TypedResponseWriter responseWriter,
      TypedStreamWriter streamWriter) {
    final DeploymentRecord value = record.getValue();
    TypedBatchWriter batchWriter = null;

    for (Workflow workflow : value.workflows()) {
      final DeployedWorkflow deployedWorkflow = workflowCache.getWorkflowByKey(workflow.getKey());
      final ExecutableStartEvent startEvent = deployedWorkflow.getWorkflow().getStartEvent();
      if (startEvent.isMessageStartEvent()) {
        final DirectBuffer messageName = startEvent.getMessageName();
        final MessageSubscriptionRecord subscription = new MessageSubscriptionRecord();
        subscription.setMessageName(messageName).setWorkflowKey(deployedWorkflow.getKey());

        if (batchWriter == null) {
          batchWriter = streamWriter.newBatch();
        }

        batchWriter.addNewCommand(MessageSubscriptionIntent.OPEN_FOR_START_EVENT, subscription);
      }
    }
  }
}
