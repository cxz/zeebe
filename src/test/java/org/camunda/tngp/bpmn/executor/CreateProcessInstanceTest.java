package org.camunda.tngp.bpmn.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.tngp.bpmn.graph.FlowElementVisitor;
import org.camunda.tngp.bpmn.graph.ProcessGraph;
import org.camunda.tngp.bpmn.graph.transformer.BpmnModelInstanceTransformer;
import org.camunda.tngp.broker.wf.runtime.WfRuntimeContext;
import org.camunda.tngp.broker.wf.runtime.log.ExecutionLogEntry;
import org.camunda.tngp.graph.bpmn.ExecutionEventType;
import org.camunda.tngp.graph.bpmn.FlowElementType;
import org.junit.Before;
import org.junit.Test;

public class CreateProcessInstanceTest
{
    FlowElementVisitor flowElementVisitor;

    BpmnExecutor executor;
    TestIdGenerator idGenerator;

    TestExecutionLog log;
    BpmnModelInstanceTransformer processGraphTransformer;

    @Before
    public void setUp()
    {
        flowElementVisitor = new FlowElementVisitor();

        idGenerator = new TestIdGenerator();
        processGraphTransformer = new BpmnModelInstanceTransformer();
        log = new TestExecutionLog();

        final WfRuntimeContext bpmnExecutorContext = new WfRuntimeContext();
        bpmnExecutorContext.setBpmnIdGenerator(idGenerator);
        bpmnExecutorContext.setExecutionLog(log);

        executor = new BpmnExecutor(bpmnExecutorContext);
    }

    @Test
    public void shouldCreateProcInst()
    {
        // given
        final ProcessGraph processGraph = processGraphTransformer.transformSingleProcess(Bpmn.createExecutableProcess().startEvent().done());

        // if
        executor.createProcessInstanceAtInitial(processGraph, 0);

        // then
        final List<ExecutionLogEntry> loggedEvents = log.getLoggedEvents();
        assertThat(loggedEvents.size()).isEqualTo(2);
        flowElementVisitor.init(processGraph);

        ExecutionLogEntry event1 = loggedEvents.get(0);
        flowElementVisitor.moveToNode(0);
        assertThat(event1.key()).isEqualTo(0);
        assertThat(event1.event()).isEqualTo(ExecutionEventType.PROC_INST_CREATED);
        assertThat(event1.flowElementId()).isEqualTo(flowElementVisitor.nodeId());
        assertThat(event1.flowElementType()).isEqualTo(FlowElementType.PROCESS);
        assertThat(event1.parentFlowElementInstanceId()).isEqualTo(-1);
        assertThat(event1.processId()).isEqualTo(processGraph.id());
        assertThat(event1.processInstanceId()).isEqualTo(0);

        ExecutionLogEntry event2 = loggedEvents.get(1);
        flowElementVisitor.moveToNode(1);
        assertThat(event2.event()).isEqualTo(ExecutionEventType.EVT_OCCURRED);
        assertThat(event2.flowElementId()).isEqualTo(flowElementVisitor.nodeId());
        assertThat(event2.flowElementType()).isEqualTo(FlowElementType.START_EVENT);
        assertThat(event2.parentFlowElementInstanceId()).isEqualTo(event1.key());
        assertThat(event2.processId()).isEqualTo(processGraph.id());
        assertThat(event2.processInstanceId()).isEqualTo(0);
    }
}