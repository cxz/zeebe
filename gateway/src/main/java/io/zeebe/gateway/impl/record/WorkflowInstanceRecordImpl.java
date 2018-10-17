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
package io.zeebe.gateway.impl.record;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.zeebe.gateway.api.record.WorkflowInstanceRecord;
import io.zeebe.gateway.impl.data.PayloadField;
import io.zeebe.gateway.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.gateway.impl.event.WorkflowInstanceEventImpl;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import java.io.InputStream;
import java.util.Map;

public abstract class WorkflowInstanceRecordImpl extends RecordImpl
    implements WorkflowInstanceRecord {
  private String bpmnProcessId;
  private int version = -1;
  private long workflowKey = -1L;
  private long workflowInstanceKey = -1L;
  private long scopeInstanceKey = -1;
  private String elementId;
  private PayloadField payload;

  public WorkflowInstanceRecordImpl(ZeebeObjectMapperImpl objectMapper, RecordType recordType) {
    super(objectMapper, recordType, ValueType.WORKFLOW_INSTANCE);
    this.payload = new PayloadField(objectMapper);
  }

  public WorkflowInstanceRecordImpl(
      WorkflowInstanceRecordImpl base, WorkflowInstanceIntent intent) {
    super(base, intent);

    this.bpmnProcessId = base.getBpmnProcessId();
    this.version = base.getVersion();
    this.workflowKey = base.getWorkflowKey();
    this.workflowInstanceKey = base.getWorkflowInstanceKey();
    this.elementId = base.getElementId();
    this.scopeInstanceKey = base.getScopeInstanceKey();

    this.payload = new PayloadField(base.payload);
  }

  @Override
  public String getBpmnProcessId() {
    return bpmnProcessId;
  }

  public void setBpmnProcessId(String bpmnProcessId) {
    this.bpmnProcessId = bpmnProcessId;
  }

  @Override
  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public long getWorkflowInstanceKey() {
    return workflowInstanceKey;
  }

  public void setWorkflowInstanceKey(long workflowInstanceKey) {
    this.workflowInstanceKey = workflowInstanceKey;
  }

  @Override
  public long getScopeInstanceKey() {
    return scopeInstanceKey;
  }

  public void setScopeInstanceKey(long scopeInstanceKey) {
    this.scopeInstanceKey = scopeInstanceKey;
  }

  @Override
  public String getElementId() {
    return elementId;
  }

  public void setElementId(String elementId) {
    this.elementId = elementId;
  }

  @JsonProperty("payload")
  public PayloadField getPayloadField() {
    return payload;
  }

  @JsonProperty("payload")
  public void setPayloadField(PayloadField payload) {
    if (payload != null) {
      this.payload = payload;
    }
  }

  @Override
  public String getPayload() {
    return payload.getAsJsonString();
  }

  @JsonIgnore
  @Override
  public Map<String, Object> getPayloadAsMap() {
    return payload.getAsMap();
  }

  @JsonIgnore
  @Override
  public <T> T getPayloadAsType(Class<T> payloadType) {
    return payload.getAsType(payloadType);
  }

  public void setPayload(String jsonString) {
    this.payload.setJson(jsonString);
  }

  public void setPayload(InputStream jsonStream) {
    this.payload.setJson(jsonStream);
  }

  public void setPayload(Map<String, Object> payload) {
    this.payload.setAsMap(payload);
  }

  public void setPayload(Object payload) {
    this.payload.setAsObject(payload);
  }

  @Override
  public long getWorkflowKey() {
    return workflowKey;
  }

  public void setWorkflowKey(long workflowKey) {
    this.workflowKey = workflowKey;
  }

  @Override
  public Class<? extends RecordImpl> getEventClass() {
    return WorkflowInstanceEventImpl.class;
  }
}
