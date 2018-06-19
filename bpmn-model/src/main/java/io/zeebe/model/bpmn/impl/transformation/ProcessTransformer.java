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
package io.zeebe.model.bpmn.impl.transformation;

import io.zeebe.model.bpmn.BpmnAspect;
import io.zeebe.model.bpmn.impl.error.ErrorCollector;
import io.zeebe.model.bpmn.impl.instance.FlowElementImpl;
import io.zeebe.model.bpmn.impl.instance.ParallelGatewayImpl;
import io.zeebe.model.bpmn.impl.instance.ProcessImpl;
import io.zeebe.model.bpmn.impl.instance.StartEventImpl;
import io.zeebe.model.bpmn.impl.transformation.nodes.ExclusiveGatewayTransformer;
import io.zeebe.model.bpmn.impl.transformation.nodes.SequenceFlowTransformer;
import io.zeebe.model.bpmn.impl.transformation.nodes.task.ServiceTaskTransformer;
import io.zeebe.model.bpmn.instance.EndEvent;
import io.zeebe.model.bpmn.instance.ExclusiveGateway;
import io.zeebe.model.bpmn.instance.FlowElement;
import io.zeebe.model.bpmn.instance.FlowNode;
import io.zeebe.model.bpmn.instance.SequenceFlow;
import io.zeebe.model.bpmn.instance.ServiceTask;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agrona.DirectBuffer;

public class ProcessTransformer {
  private final SequenceFlowTransformer sequenceFlowTransformer = new SequenceFlowTransformer();
  private final ServiceTaskTransformer serviceTaskTransformer = new ServiceTaskTransformer();
  private final ExclusiveGatewayTransformer exclusiveGatewayTransformer =
      new ExclusiveGatewayTransformer();

  public void transform(ErrorCollector errorCollector, ProcessImpl process) {
    final List<FlowElementImpl> flowElements = process.collectFlowElements();
    process.getFlowElements().addAll(flowElements);

    final Map<DirectBuffer, FlowElementImpl> flowElementsById = getFlowElementsById(flowElements);
    process.getFlowElementMap().putAll(flowElementsById);

    setInitialStartEvent(process);

    sequenceFlowTransformer.transform(errorCollector, process.getSequenceFlows(), flowElementsById);
    serviceTaskTransformer.transform(errorCollector, process.getServiceTasks());
    exclusiveGatewayTransformer.transform(process.getExclusiveGateways());

    transformBpmnAspects(process);
  }

  private Map<DirectBuffer, FlowElementImpl> getFlowElementsById(
      List<FlowElementImpl> flowElements) {
    final Map<DirectBuffer, FlowElementImpl> map = new HashMap<>();
    for (FlowElementImpl flowElement : flowElements) {
      map.put(flowElement.getIdAsBuffer(), flowElement);
    }
    return map;
  }

  private void setInitialStartEvent(final ProcessImpl process) {
    final List<StartEventImpl> startEvents = process.getStartEvents();
    if (startEvents.size() >= 1) {
      final StartEventImpl startEvent = startEvents.get(0);
      process.setInitialStartEvent(startEvent);
    }
  }

  private void transformBpmnAspects(ProcessImpl process) {
    final List<FlowElement> flowElements = process.getFlowElements();
    for (int f = 0; f < flowElements.size(); f++) {
      final FlowElementImpl flowElement = (FlowElementImpl) flowElements.get(f);

      if (flowElement instanceof FlowNode) {
        final FlowNode flowNode = (FlowNode) flowElement;

        final List<SequenceFlow> outgoingSequenceFlows = flowNode.getOutgoingSequenceFlows();
        if (outgoingSequenceFlows.isEmpty()) {
          flowElement.setBpmnAspect(BpmnAspect.CONSUME_TOKEN);
        } else if (outgoingSequenceFlows.size() == 1
            && !outgoingSequenceFlows.get(0).hasCondition()) {
          flowElement.setBpmnAspect(BpmnAspect.TAKE_SEQUENCE_FLOW);
        } else if (flowElement instanceof ExclusiveGateway) {
          flowElement.setBpmnAspect(BpmnAspect.EXCLUSIVE_SPLIT);
        }
        else if (flowElement instanceof ParallelGatewayImpl) {
          if (((ParallelGatewayImpl) flowElement).getOutgoing().size() == 1)
          {
            flowElement.setBpmnAspect(BpmnAspect.TAKE_SEQUENCE_FLOW);
          }
          else
          {
            flowElement.setBpmnAspect(BpmnAspect.PARALLEL_SPLIT);
          }
        }
      }
      else if (flowElement instanceof SequenceFlow)
      {
        final SequenceFlow flow = (SequenceFlow) flowElement;
        final FlowNode target = flow.getTargetNode();

        if (target instanceof ExclusiveGateway)
        {
          flowElement.setBpmnAspect(BpmnAspect.ACTIVATE_GATEWAY);
        }
        else if (target instanceof ParallelGatewayImpl)
        {
          if (target.getIncomingSequenceFlows().size() == 1)
          {
            flowElement.setBpmnAspect(BpmnAspect.ACTIVATE_GATEWAY);
          }
          else
          {
            flowElement.setBpmnAspect(BpmnAspect.PARALLEL_MERGE);
          }
        }
        else if (target instanceof EndEvent)
        {
          flowElement.setBpmnAspect(BpmnAspect.TRIGGER_NONE_EVENT);
        }
        else if (target instanceof ServiceTask)
        {
          flowElement.setBpmnAspect(BpmnAspect.START_ACTIVITY);
        }
      }

    }
  }
}
