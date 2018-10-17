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
package io.zeebe.broker.it.util;

import static io.zeebe.protocol.intent.WorkflowInstanceIntent.CREATED;
import static io.zeebe.protocol.intent.WorkflowInstanceIntent.ELEMENT_ACTIVATED;
import static io.zeebe.protocol.intent.WorkflowInstanceIntent.ELEMENT_COMPLETED;
import static io.zeebe.protocol.intent.WorkflowInstanceIntent.ELEMENT_READY;
import static io.zeebe.protocol.intent.WorkflowInstanceIntent.ELEMENT_TERMINATED;
import static io.zeebe.protocol.intent.WorkflowInstanceIntent.END_EVENT_OCCURRED;
import static io.zeebe.protocol.intent.WorkflowInstanceIntent.PAYLOAD_UPDATED;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.value.JobRecordValue;
import io.zeebe.exporter.record.value.WorkflowInstanceRecordValue;
import io.zeebe.protocol.intent.IncidentIntent;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.function.Consumer;

public class ZeebeAssertHelper {

  public static void assertWorkflowInstanceCreated() {
    assertWorkflowInstanceCreated((e) -> {});
  }

  public static void assertWorkflowInstanceCreated(long workflowInstanceKey) {
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.CREATED)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .exists())
        .isTrue();
    assertWorkflowInstanceState(CREATED, (e) -> {});
  }

  public static void assertWorkflowInstanceCreated(Consumer<WorkflowInstanceRecordValue> consumer) {
    assertWorkflowInstanceState(CREATED, consumer);
  }

  public static void assertJobCreated(String jobType) {
    assertThat(RecordingExporter.jobRecords(JobIntent.CREATED).withType(jobType).exists()).isTrue();
  }

  public static void assertJobCreated(String jobType, Consumer<JobRecordValue> consumer) {
    final JobRecordValue value =
        RecordingExporter.jobRecords(JobIntent.CREATED)
            .withType(jobType)
            .findFirst()
            .get()
            .getValue();

    assertThat(value).isNotNull();

    consumer.accept(value);
  }

  public static void assertIncidentCreated() {
    assertThat(RecordingExporter.incidentRecords(IncidentIntent.CREATED).exists());
  }

  public static void assertElementActivated(String element) {
    assertElementInState(ELEMENT_ACTIVATED, element, (e) -> {});
  }

  public static void assertElementReady(String element) {
    assertElementInState(ELEMENT_READY, element, (e) -> {});
  }

  public static void assertEndEventOccurred(String activityId) {
    assertElementInState(END_EVENT_OCCURRED, activityId, (e) -> {});
  }

  public static void assertWorkflowInstanceCanceled(String bpmnId) {
    assertThat(
            RecordingExporter.workflowInstanceRecords(ELEMENT_TERMINATED)
                .withBpmnProcessId(bpmnId)
                .withActivityId(bpmnId)
                .exists())
        .isTrue();
  }

  public static void assertWorkflowInstanceCompleted(String workflow, long workflowInstanceKey) {
    assertElementCompleted(workflowInstanceKey, workflow, (e) -> {});
  }

  public static void assertWorkflowInstanceCompleted(String bpmnId) {
    assertWorkflowInstanceCompleted(bpmnId, (e) -> {});
  }

  public static void assertWorkflowInstanceCompleted(
      String bpmnId, Consumer<WorkflowInstanceRecordValue> eventConsumer) {
    assertElementCompleted(bpmnId, bpmnId, eventConsumer);
  }

  public static void assertJobCompleted() {
    assertThat(RecordingExporter.jobRecords(JobIntent.COMPLETED).exists()).isTrue();
  }

  public static void assertJobCompleted(String jobType) {
    assertJobCompleted(jobType, (j) -> {});
  }

  public static void assertJobCompleted(String jobType, Consumer<JobRecordValue> consumer) {
    final JobRecordValue job =
        RecordingExporter.jobRecords(JobIntent.COMPLETED)
            .withType(jobType)
            .findFirst()
            .get()
            .getValue();

    assertThat(job).isNotNull();
    consumer.accept(job);
  }

  public static void assertElementCompleted(String bpmnId, String activity) {
    assertElementCompleted(bpmnId, activity, (e) -> {});
  }

  public static void assertElementCompleted(
      String bpmnId, String activity, Consumer<WorkflowInstanceRecordValue> eventConsumer) {
    final Record<WorkflowInstanceRecordValue> workflowInstanceRecordValueRecord =
        RecordingExporter.workflowInstanceRecords(ELEMENT_COMPLETED)
            .withBpmnProcessId(bpmnId)
            .withActivityId(activity)
            .findFirst()
            .get();

    assertThat(workflowInstanceRecordValueRecord).isNotNull();

    eventConsumer.accept(workflowInstanceRecordValueRecord.getValue());
  }

  public static void assertElementCompleted(
      long workflowInstanceKey,
      String activity,
      Consumer<WorkflowInstanceRecordValue> eventConsumer) {
    final Record<WorkflowInstanceRecordValue> workflowInstanceRecordValueRecord =
        RecordingExporter.workflowInstanceRecords(ELEMENT_COMPLETED)
            .withActivityId(activity)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .findFirst()
            .get();

    assertThat(workflowInstanceRecordValueRecord).isNotNull();

    eventConsumer.accept(workflowInstanceRecordValueRecord.getValue());
  }

  public static void assertWorkflowInstancePayloadUpdated() {
    assertWorkflowInstancePayloadUpdated((e) -> {});
  }

  public static void assertWorkflowInstancePayloadUpdated(
      Consumer<WorkflowInstanceRecordValue> eventConsumer) {
    assertWorkflowInstanceState(PAYLOAD_UPDATED, eventConsumer);
  }

  public static void assertWorkflowInstanceState(
      WorkflowInstanceIntent intent, Consumer<WorkflowInstanceRecordValue> consumer) {
    final WorkflowInstanceRecordValue value =
        RecordingExporter.workflowInstanceRecords(intent).findFirst().get().getValue();

    assertThat(value).isNotNull();

    consumer.accept(value);
  }

  public static void assertElementInState(
      WorkflowInstanceIntent intent,
      String element,
      Consumer<WorkflowInstanceRecordValue> consumer) {
    final WorkflowInstanceRecordValue value =
        RecordingExporter.workflowInstanceRecords(intent)
            .withActivityId(element)
            .findFirst()
            .get()
            .getValue();

    assertThat(value).isNotNull();

    consumer.accept(value);
  }

  public static void assertIncidentResolved() {
    assertThat(RecordingExporter.incidentRecords(IncidentIntent.RESOLVED).exists()).isTrue();
  }

  public static void assertIncidentResolveFailed() {
    assertThat(RecordingExporter.incidentRecords(IncidentIntent.RESOLVE_FAILED).exists()).isTrue();
  }

  public static void assertIncidentDeleted() {
    assertThat(RecordingExporter.incidentRecords(IncidentIntent.DELETED).exists()).isTrue();
  }
}
