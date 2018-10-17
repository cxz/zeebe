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
package io.zeebe.test.util.record;

import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.value.WorkflowInstanceRecordValue;
import java.util.stream.Stream;

public class WorkflowInstanceRecordStream
    extends ExporterRecordWithPayloadStream<
        WorkflowInstanceRecordValue, WorkflowInstanceRecordStream> {

  public WorkflowInstanceRecordStream(final Stream<Record<WorkflowInstanceRecordValue>> stream) {
    super(stream);
  }

  @Override
  protected WorkflowInstanceRecordStream supply(
      final Stream<Record<WorkflowInstanceRecordValue>> stream) {
    return new WorkflowInstanceRecordStream(stream);
  }

  public WorkflowInstanceRecordStream withBpmnProcessId(final String bpmnProcessId) {
    return valueFilter(v -> bpmnProcessId.equals(v.getBpmnProcessId()));
  }

  public WorkflowInstanceRecordStream withVersion(final int version) {
    return valueFilter(v -> v.getVersion() == version);
  }

  public WorkflowInstanceRecordStream withWorkflowKey(final long workflowKey) {
    return valueFilter(v -> v.getWorkflowKey() == workflowKey);
  }

  public WorkflowInstanceRecordStream withWorkflowInstanceKey(final long workflowInstanceKey) {
    return valueFilter(v -> v.getWorkflowInstanceKey() == workflowInstanceKey);
  }

  public WorkflowInstanceRecordStream withElementId(final String elementId) {
    return valueFilter(v -> elementId.equals(v.getElementId()));
  }

  public WorkflowInstanceRecordStream withScopeInstanceKey(final long scopeInstanceKey) {
    return valueFilter(v -> v.getScopeInstanceKey() == scopeInstanceKey);
  }
}
