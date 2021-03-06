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
package io.zeebe.broker.workflow.processor.instance;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.util.metrics.Metric;
import io.zeebe.util.metrics.MetricsManager;

public final class WorkflowInstanceCreatedEventProcessor
    implements TypedRecordProcessor<WorkflowInstanceRecord> {

  private final WorkflowState workflowState;
  private Metric workflowInstanceEventCreate;

  public WorkflowInstanceCreatedEventProcessor(final WorkflowState workflowState) {
    this.workflowState = workflowState;
  }

  @Override
  public void onOpen(final TypedStreamProcessor streamProcessor) {
    final MetricsManager metricsManager =
        streamProcessor.getStreamProcessorContext().getActorScheduler().getMetricsManager();

    final LogStream logStream = streamProcessor.getEnvironment().getStream();
    final String partitionId = Integer.toString(logStream.getPartitionId());

    workflowInstanceEventCreate =
        metricsManager
            .newMetric("workflow_instance_events_count")
            .type("counter")
            .label("partition", partitionId)
            .label("type", "created")
            .create();
  }

  @Override
  public void onClose() {
    workflowInstanceEventCreate.close();
  }

  @Override
  public void processRecord(
      final TypedRecord<WorkflowInstanceRecord> record,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter) {
    workflowInstanceEventCreate.incrementOrdered();
    responseWriter.writeEvent(record);

    workflowState
        .getElementInstanceState()
        .newInstance(record.getKey(), record.getValue(), WorkflowInstanceIntent.ELEMENT_READY);
  }
}
