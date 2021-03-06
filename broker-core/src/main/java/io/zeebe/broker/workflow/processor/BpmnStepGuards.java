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
package io.zeebe.broker.workflow.processor;

import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Predicate;

public class BpmnStepGuards {

  private final Map<WorkflowInstanceIntent, Predicate<BpmnStepContext>> stepGuards =
      new EnumMap<>(WorkflowInstanceIntent.class);

  // when element or scope element instance have been completed/terminated already
  private final Predicate<BpmnStepContext> hasElementInstances =
      c -> c.getElementInstance() != null || c.getFlowScopeInstance() != null;

  public BpmnStepGuards() {

    // step guards: should a record in a certain state be handled?
    final Predicate<BpmnStepContext> noConcurrentTransitionGuard =
        c -> c.getState() == c.getElementInstance().getState();
    final Predicate<BpmnStepContext> scopeActiveGuard =
        c ->
            c.getFlowScopeInstance() != null
                && c.getFlowScopeInstance().getState() == WorkflowInstanceIntent.ELEMENT_ACTIVATED;
    final Predicate<BpmnStepContext> scopeTerminatingGuard =
        c ->
            c.getFlowScopeInstance() != null
                && c.getFlowScopeInstance().getState()
                    == WorkflowInstanceIntent.ELEMENT_TERMINATING;

    stepGuards.put(WorkflowInstanceIntent.ELEMENT_READY, noConcurrentTransitionGuard);
    stepGuards.put(WorkflowInstanceIntent.ELEMENT_ACTIVATED, noConcurrentTransitionGuard);
    stepGuards.put(WorkflowInstanceIntent.ELEMENT_COMPLETING, noConcurrentTransitionGuard);
    stepGuards.put(WorkflowInstanceIntent.ELEMENT_COMPLETED, scopeActiveGuard);
    stepGuards.put(WorkflowInstanceIntent.ELEMENT_TERMINATING, c -> true);
    stepGuards.put(WorkflowInstanceIntent.ELEMENT_TERMINATED, scopeTerminatingGuard);

    stepGuards.put(WorkflowInstanceIntent.END_EVENT_OCCURRED, scopeActiveGuard);
    stepGuards.put(WorkflowInstanceIntent.GATEWAY_ACTIVATED, scopeActiveGuard);
    stepGuards.put(WorkflowInstanceIntent.START_EVENT_OCCURRED, scopeActiveGuard);
    stepGuards.put(WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, scopeActiveGuard);
  }

  public boolean shouldHandle(BpmnStepContext context) {
    final Predicate<BpmnStepContext> guard = stepGuards.get(context.getState());
    return hasElementInstances.test(context) && guard.test(context);
  }
}
