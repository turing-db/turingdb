#include "Executor.h"

#include "CallEdgeTypeStep.h"
#include "CallLabelStep.h"
#include "Pipeline.h"
#include "PipelineException.h"
#include "PipelineMacros.h"
#include "Profiler.h"

using namespace db;

Executor::Executor() 
{
    init();
}

Executor::~Executor() {
}

void Executor::init() {
    runImpl(nullptr, nullptr, true);
}

void Executor::run(ExecutionContext* ctxt, Pipeline* pipeline) {
    Profile profile {"Executor::run"};
    runImpl(ctxt, pipeline);
}

void Executor::runImpl(ExecutionContext* ctxt, Pipeline* pipeline, bool init) {
    if (init) {
        _activateTbl.fill(nullptr);
        _returnTbl.fill(nullptr);

        // ACTIVATE jump table
        _activateTbl[(uint64_t)PipelineOpcode::STOP] = GOTOPTR(StopStep);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODES] = ACTIVATE_PTR(ScanNodesStep);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODES_BY_LABEL] = ACTIVATE_PTR(ScanNodesByLabelStep);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_EDGES] = ACTIVATE_PTR(ScanEdgesStep);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_IN_EDGES_BY_LABEL] = ACTIVATE_PTR(ScanInEdgesByLabelStep);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_OUT_EDGES_BY_LABEL] = ACTIVATE_PTR(ScanOutEdgesByLabelStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_OUT_EDGES] = ACTIVATE_PTR(GetOutEdgesStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_LABELSETID_STEP] = ACTIVATE_PTR(GetLabelSetIDStep);
        _activateTbl[(uint64_t)PipelineOpcode::FILTER] = ACTIVATE_PTR(FilterStep);
        _activateTbl[(uint64_t)PipelineOpcode::TRANSFORM] = ACTIVATE_PTR(TransformStep);
        _activateTbl[(uint64_t)PipelineOpcode::COUNT] = ACTIVATE_PTR(CountStep);
        _activateTbl[(uint64_t)PipelineOpcode::LAMBDA] = ACTIVATE_PTR(LambdaStep);
        _activateTbl[(uint64_t)PipelineOpcode::CREATE_GRAPH] = ACTIVATE_PTR(CreateGraphStep);
        _activateTbl[(uint64_t)PipelineOpcode::LIST_GRAPH] = ACTIVATE_PTR(ListGraphStep);
        _activateTbl[(uint64_t)PipelineOpcode::LOAD_GRAPH] = ACTIVATE_PTR(LoadGraphStep);
        _activateTbl[(uint64_t)PipelineOpcode::QUERY_NODE_INDEX] = ACTIVATE_PTR(QueryNodeIndexStep);
        _activateTbl[(uint64_t)PipelineOpcode::QUERY_EDGE_INDEX] = ACTIVATE_PTR(QueryEdgeIndexStep);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_INT64] = ACTIVATE_PTR(ScanNodesByPropertyInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_UINT64] = ACTIVATE_PTR(ScanNodesByPropertyUInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_DOUBLE] = ACTIVATE_PTR(ScanNodesByPropertyDoubleStep);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_STRING_APPROX] = ACTIVATE_PTR(ScanNodesStringApproxStep);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_STRING] = ACTIVATE_PTR(ScanNodesByPropertyStringStep);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_BOOL] = ACTIVATE_PTR(ScanNodesByPropertyBoolStep);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_AND_LABEL_INT64] = ACTIVATE_PTR(ScanNodesByPropertyAndLabelInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_AND_LABEL_UINT64] = ACTIVATE_PTR(ScanNodesByPropertyAndLabelUInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_AND_LABEL_DOUBLE] = ACTIVATE_PTR(ScanNodesByPropertyAndLabelDoubleStep);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_AND_LABEL_STRING] = ACTIVATE_PTR(ScanNodesByPropertyAndLabelStringStep);
        _activateTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_AND_LABEL_BOOL] = ACTIVATE_PTR(ScanNodesByPropertyAndLabelBoolStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_NODE_PROPERTY_INT64] = ACTIVATE_PTR(GetNodePropertyInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::GET_NODE_PROPERTY_UINT64] = ACTIVATE_PTR(GetNodePropertyUInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::GET_NODE_PROPERTY_DOUBLE] = ACTIVATE_PTR(GetNodePropertyDoubleStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_NODE_PROPERTY_STRING] = ACTIVATE_PTR(GetNodePropertyStringStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_NODE_PROPERTY_BOOL] = ACTIVATE_PTR(GetNodePropertyBoolStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_EDGE_PROPERTY_INT64] = ACTIVATE_PTR(GetEdgePropertyInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::GET_EDGE_PROPERTY_UINT64] = ACTIVATE_PTR(GetEdgePropertyUInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::GET_EDGE_PROPERTY_DOUBLE] = ACTIVATE_PTR(GetEdgePropertyDoubleStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_EDGE_PROPERTY_STRING] = ACTIVATE_PTR(GetEdgePropertyStringStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_EDGE_PROPERTY_BOOL] = ACTIVATE_PTR(GetEdgePropertyBoolStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_FILTERED_NODE_PROPERTY_INT64] = ACTIVATE_PTR(GetFilteredNodePropertyInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::GET_FILTERED_NODE_PROPERTY_UINT64] = ACTIVATE_PTR(GetFilteredNodePropertyUInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::GET_FILTERED_NODE_PROPERTY_DOUBLE] = ACTIVATE_PTR(GetFilteredNodePropertyDoubleStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_FILTERED_NODE_PROPERTY_STRING] = ACTIVATE_PTR(GetFilteredNodePropertyStringStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_FILTERED_NODE_PROPERTY_BOOL] = ACTIVATE_PTR(GetFilteredNodePropertyBoolStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_FILTERED_EDGE_PROPERTY_INT64] = ACTIVATE_PTR(GetFilteredEdgePropertyInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::GET_FILTERED_EDGE_PROPERTY_UINT64] = ACTIVATE_PTR(GetFilteredEdgePropertyUInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::GET_FILTERED_EDGE_PROPERTY_DOUBLE] = ACTIVATE_PTR(GetFilteredEdgePropertyDoubleStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_FILTERED_EDGE_PROPERTY_STRING] = ACTIVATE_PTR(GetFilteredEdgePropertyStringStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_FILTERED_EDGE_PROPERTY_BOOL] = ACTIVATE_PTR(GetFilteredEdgePropertyBoolStep);
        _activateTbl[(uint64_t)PipelineOpcode::HISTORY] = ACTIVATE_PTR(HistoryStep);
        _activateTbl[(uint64_t)PipelineOpcode::CHANGE] = ACTIVATE_PTR(ChangeStep);
        _activateTbl[(uint64_t)PipelineOpcode::CREATE_NODE] = ACTIVATE_PTR(CreateNodeStep);
        _activateTbl[(uint64_t)PipelineOpcode::CREATE_EDGE] = ACTIVATE_PTR(CreateEdgeStep);
        _activateTbl[(uint64_t)PipelineOpcode::COMMIT] = ACTIVATE_PTR(CommitStep);
        _activateTbl[(uint64_t)PipelineOpcode::CALL_PROPERTIES] = ACTIVATE_PTR(CallPropertyStep);
        _activateTbl[(uint64_t)PipelineOpcode::CALL_LABELS] = ACTIVATE_PTR(CallLabelStep);
        _activateTbl[(uint64_t)PipelineOpcode::CALL_EDGETYPES] = ACTIVATE_PTR(CallEdgeTypeStep);
        _activateTbl[(uint64_t)PipelineOpcode::CALL_LABELSETS] = ACTIVATE_PTR(CallLabelSetStep);
        _activateTbl[(uint64_t)PipelineOpcode::END] = ACTIVATE_END_PTR();

        // RETURN jump table
        _returnTbl[(uint64_t)PipelineOpcode::STOP] = GOTOPTR(StopStep);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODES] = RETURN_PTR(ScanNodesStep);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODES_BY_LABEL] = RETURN_PTR(ScanNodesByLabelStep);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_EDGES] = RETURN_PTR(ScanEdgesStep);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_IN_EDGES_BY_LABEL] = RETURN_PTR(ScanInEdgesByLabelStep);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_OUT_EDGES_BY_LABEL] = RETURN_PTR(ScanOutEdgesByLabelStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_OUT_EDGES] = RETURN_PTR(GetOutEdgesStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_LABELSETID_STEP] = RETURN_PTR(GetLabelSetIDStep);
        _returnTbl[(uint64_t)PipelineOpcode::FILTER] = RETURN_PTR(FilterStep);
        _returnTbl[(uint64_t)PipelineOpcode::TRANSFORM] = RETURN_PTR(TransformStep);
        _returnTbl[(uint64_t)PipelineOpcode::COUNT] = RETURN_PTR(CountStep);
        _returnTbl[(uint64_t)PipelineOpcode::LAMBDA] = RETURN_PTR(LambdaStep);
        _returnTbl[(uint64_t)PipelineOpcode::CREATE_GRAPH] = RETURN_PTR(CreateGraphStep);
        _returnTbl[(uint64_t)PipelineOpcode::LIST_GRAPH] = RETURN_PTR(ListGraphStep);
        _returnTbl[(uint64_t)PipelineOpcode::LOAD_GRAPH] = RETURN_PTR(LoadGraphStep);
        _returnTbl[(uint64_t)PipelineOpcode::QUERY_NODE_INDEX] = RETURN_PTR(QueryNodeIndexStep);
        _returnTbl[(uint64_t)PipelineOpcode::QUERY_EDGE_INDEX] = RETURN_PTR(QueryEdgeIndexStep);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_INT64] = RETURN_PTR(ScanNodesByPropertyInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_UINT64] = RETURN_PTR(ScanNodesByPropertyUInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_DOUBLE] = RETURN_PTR(ScanNodesByPropertyDoubleStep);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_STRING_APPROX] = RETURN_PTR(ScanNodesStringApproxStep);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_STRING] = RETURN_PTR(ScanNodesByPropertyStringStep);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_BOOL] = RETURN_PTR(ScanNodesByPropertyBoolStep);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_AND_LABEL_INT64] = RETURN_PTR(ScanNodesByPropertyAndLabelInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_AND_LABEL_UINT64] = RETURN_PTR(ScanNodesByPropertyAndLabelUInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_AND_LABEL_DOUBLE] = RETURN_PTR(ScanNodesByPropertyAndLabelDoubleStep);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_AND_LABEL_STRING] = RETURN_PTR(ScanNodesByPropertyAndLabelStringStep);
        _returnTbl[(uint64_t)PipelineOpcode::SCAN_NODE_PROPERTY_AND_LABEL_BOOL] = RETURN_PTR(ScanNodesByPropertyAndLabelBoolStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_NODE_PROPERTY_INT64] = RETURN_PTR(GetNodePropertyInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::GET_NODE_PROPERTY_UINT64] = RETURN_PTR(GetNodePropertyUInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::GET_NODE_PROPERTY_DOUBLE] = RETURN_PTR(GetNodePropertyDoubleStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_NODE_PROPERTY_STRING] = RETURN_PTR(GetNodePropertyStringStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_NODE_PROPERTY_BOOL] = RETURN_PTR(GetNodePropertyBoolStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_EDGE_PROPERTY_INT64] = RETURN_PTR(GetEdgePropertyInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::GET_EDGE_PROPERTY_UINT64] = RETURN_PTR(GetEdgePropertyUInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::GET_EDGE_PROPERTY_DOUBLE] = RETURN_PTR(GetEdgePropertyDoubleStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_EDGE_PROPERTY_STRING] = RETURN_PTR(GetEdgePropertyStringStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_EDGE_PROPERTY_BOOL] = RETURN_PTR(GetEdgePropertyBoolStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_FILTERED_NODE_PROPERTY_INT64] = RETURN_PTR(GetFilteredNodePropertyInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::GET_FILTERED_NODE_PROPERTY_UINT64] = RETURN_PTR(GetFilteredNodePropertyUInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::GET_FILTERED_NODE_PROPERTY_DOUBLE] = RETURN_PTR(GetFilteredNodePropertyDoubleStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_FILTERED_NODE_PROPERTY_STRING] = RETURN_PTR(GetFilteredNodePropertyStringStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_FILTERED_NODE_PROPERTY_BOOL] = RETURN_PTR(GetFilteredNodePropertyBoolStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_FILTERED_EDGE_PROPERTY_INT64] = RETURN_PTR(GetFilteredEdgePropertyInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::GET_FILTERED_EDGE_PROPERTY_UINT64] = RETURN_PTR(GetFilteredEdgePropertyUInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::GET_FILTERED_EDGE_PROPERTY_DOUBLE] = RETURN_PTR(GetFilteredEdgePropertyDoubleStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_FILTERED_EDGE_PROPERTY_STRING] = RETURN_PTR(GetFilteredEdgePropertyStringStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_FILTERED_EDGE_PROPERTY_BOOL] = RETURN_PTR(GetFilteredEdgePropertyBoolStep);
        _returnTbl[(uint64_t)PipelineOpcode::HISTORY] = RETURN_PTR(HistoryStep);
        _returnTbl[(uint64_t)PipelineOpcode::CHANGE] = RETURN_PTR(ChangeStep);
        _returnTbl[(uint64_t)PipelineOpcode::CREATE_NODE] = RETURN_PTR(CreateNodeStep);
        _returnTbl[(uint64_t)PipelineOpcode::CREATE_EDGE] = RETURN_PTR(CreateEdgeStep);
        _returnTbl[(uint64_t)PipelineOpcode::COMMIT] = RETURN_PTR(CommitStep);
        _returnTbl[(uint64_t)PipelineOpcode::CALL_PROPERTIES] = RETURN_PTR(CallPropertyStep);
        _returnTbl[(uint64_t)PipelineOpcode::CALL_LABELS] = RETURN_PTR(CallLabelStep);
        _returnTbl[(uint64_t)PipelineOpcode::CALL_EDGETYPES] = RETURN_PTR(CallEdgeTypeStep);
        _returnTbl[(uint64_t)PipelineOpcode::CALL_LABELSETS] = RETURN_PTR(CallLabelSetStep);
        _returnTbl[(uint64_t)PipelineOpcode::END] = GOTOPTR(StopStep);

        checkJumpTables();

        return;
    }

    const auto& activateTbl = _activateTbl;
    const auto& returnTbl = _returnTbl;

    const auto& steps = pipeline->steps();

    // Check that the pipeline has at least a size of 2 steps (needs STOP and END)
    if (steps.size() < 2) {
        throw PipelineException("Pipeline size is less than 2");
    }

    // Check that the first step is a STOP step
    if (steps.front().getOpcode() != PipelineOpcode::STOP) {
        throw PipelineException("Pipeline has no STOP step");
    }

    // Check that the last step is an END step
    if (steps.back().getOpcode() != PipelineOpcode::END) {
        throw PipelineException("Pipeline has no END step");
    }

    // Prepare steps
    for (PipelineStep& step : pipeline->steps()) {
        step.prepare(ctxt);
    }

    PipelineStep* pipeStep = steps.data();

    // Go to first step
    pipeStep++;
    goto *_activateTbl[(uint64_t)pipeStep->getOpcode()];

    // Stop step that marks the beginning of the pipeline
    GOTOCASE(StopStep, {
        goto ExecutorExit;
    })

    // ACTIVATE actions
    ACTIVATE_STEP(ScanNodesStringApproxStep)
    ACTIVATE_STEP(ScanNodesStep)
    ACTIVATE_STEP(ScanNodesByLabelStep)
    ACTIVATE_STEP(ScanEdgesStep)
    ACTIVATE_STEP(ScanInEdgesByLabelStep)
    ACTIVATE_STEP(ScanOutEdgesByLabelStep)
    ACTIVATE_STEP(GetOutEdgesStep)
    ACTIVATE_STEP(GetLabelSetIDStep)
    ACTIVATE_STEP(FilterStep)
    ACTIVATE_STEP(TransformStep)
    ACTIVATE_STEP(CountStep)
    ACTIVATE_STEP(LambdaStep)
    ACTIVATE_STEP(CreateGraphStep)
    ACTIVATE_STEP(ListGraphStep)
    ACTIVATE_STEP(LoadGraphStep)
    ACTIVATE_STEP(QueryNodeIndexStep)
    ACTIVATE_STEP(QueryEdgeIndexStep)
    ACTIVATE_STEP(ScanNodesByPropertyInt64Step)
    ACTIVATE_STEP(ScanNodesByPropertyUInt64Step)
    ACTIVATE_STEP(ScanNodesByPropertyDoubleStep)
    ACTIVATE_STEP(ScanNodesByPropertyStringStep)
    ACTIVATE_STEP(ScanNodesByPropertyBoolStep)
    ACTIVATE_STEP(ScanNodesByPropertyAndLabelInt64Step)
    ACTIVATE_STEP(ScanNodesByPropertyAndLabelUInt64Step)
    ACTIVATE_STEP(ScanNodesByPropertyAndLabelDoubleStep)
    ACTIVATE_STEP(ScanNodesByPropertyAndLabelStringStep)
    ACTIVATE_STEP(ScanNodesByPropertyAndLabelBoolStep)
    ACTIVATE_STEP(GetNodePropertyInt64Step)
    ACTIVATE_STEP(GetNodePropertyUInt64Step)
    ACTIVATE_STEP(GetNodePropertyDoubleStep)
    ACTIVATE_STEP(GetNodePropertyStringStep)
    ACTIVATE_STEP(GetNodePropertyBoolStep)
    ACTIVATE_STEP(GetEdgePropertyInt64Step)
    ACTIVATE_STEP(GetEdgePropertyUInt64Step)
    ACTIVATE_STEP(GetEdgePropertyDoubleStep)
    ACTIVATE_STEP(GetEdgePropertyStringStep)
    ACTIVATE_STEP(GetEdgePropertyBoolStep)
    ACTIVATE_STEP(GetFilteredNodePropertyInt64Step)
    ACTIVATE_STEP(GetFilteredNodePropertyUInt64Step)
    ACTIVATE_STEP(GetFilteredNodePropertyDoubleStep)
    ACTIVATE_STEP(GetFilteredNodePropertyStringStep)
    ACTIVATE_STEP(GetFilteredNodePropertyBoolStep)
    ACTIVATE_STEP(GetFilteredEdgePropertyInt64Step)
    ACTIVATE_STEP(GetFilteredEdgePropertyUInt64Step)
    ACTIVATE_STEP(GetFilteredEdgePropertyDoubleStep)
    ACTIVATE_STEP(GetFilteredEdgePropertyStringStep)
    ACTIVATE_STEP(GetFilteredEdgePropertyBoolStep)
    ACTIVATE_STEP(HistoryStep)
    ACTIVATE_STEP(ChangeStep)
    ACTIVATE_STEP(CreateNodeStep)
    ACTIVATE_STEP(CreateEdgeStep)
    ACTIVATE_STEP(CommitStep)
    ACTIVATE_STEP(CallPropertyStep)
    ACTIVATE_STEP(CallLabelStep)
    ACTIVATE_STEP(CallEdgeTypeStep)
    ACTIVATE_STEP(CallLabelSetStep)
    ACTIVATE_END()

    // RETURN actions
    RETURN_STEP(ScanNodesStringApproxStep)
    RETURN_STEP(ScanNodesStep)
    RETURN_STEP(ScanNodesByLabelStep)
    RETURN_STEP(ScanEdgesStep)
    RETURN_STEP(ScanInEdgesByLabelStep)
    RETURN_STEP(ScanOutEdgesByLabelStep)
    RETURN_STEP(GetOutEdgesStep)
    RETURN_STEP(GetLabelSetIDStep)
    RETURN_STEP(FilterStep)
    RETURN_STEP(TransformStep)
    RETURN_STEP(CountStep)
    RETURN_STEP(LambdaStep)
    RETURN_STEP(CreateGraphStep)
    RETURN_STEP(ListGraphStep)
    RETURN_STEP(LoadGraphStep)
    RETURN_STEP(QueryNodeIndexStep)
    RETURN_STEP(QueryEdgeIndexStep)
    RETURN_STEP(ScanNodesByPropertyInt64Step)
    RETURN_STEP(ScanNodesByPropertyUInt64Step)
    RETURN_STEP(ScanNodesByPropertyDoubleStep)
    RETURN_STEP(ScanNodesByPropertyStringStep)
    RETURN_STEP(ScanNodesByPropertyBoolStep)
    RETURN_STEP(ScanNodesByPropertyAndLabelInt64Step)
    RETURN_STEP(ScanNodesByPropertyAndLabelUInt64Step)
    RETURN_STEP(ScanNodesByPropertyAndLabelDoubleStep)
    RETURN_STEP(ScanNodesByPropertyAndLabelStringStep)
    RETURN_STEP(ScanNodesByPropertyAndLabelBoolStep)
    RETURN_STEP(GetNodePropertyInt64Step)
    RETURN_STEP(GetNodePropertyUInt64Step)
    RETURN_STEP(GetNodePropertyDoubleStep)
    RETURN_STEP(GetNodePropertyStringStep)
    RETURN_STEP(GetNodePropertyBoolStep)
    RETURN_STEP(GetEdgePropertyInt64Step)
    RETURN_STEP(GetEdgePropertyUInt64Step)
    RETURN_STEP(GetEdgePropertyDoubleStep)
    RETURN_STEP(GetEdgePropertyStringStep)
    RETURN_STEP(GetEdgePropertyBoolStep)
    RETURN_STEP(GetFilteredNodePropertyInt64Step)
    RETURN_STEP(GetFilteredNodePropertyUInt64Step)
    RETURN_STEP(GetFilteredNodePropertyDoubleStep)
    RETURN_STEP(GetFilteredNodePropertyStringStep)
    RETURN_STEP(GetFilteredNodePropertyBoolStep)
    RETURN_STEP(GetFilteredEdgePropertyInt64Step)
    RETURN_STEP(GetFilteredEdgePropertyUInt64Step)
    RETURN_STEP(GetFilteredEdgePropertyDoubleStep)
    RETURN_STEP(GetFilteredEdgePropertyStringStep)
    RETURN_STEP(GetFilteredEdgePropertyBoolStep)
    RETURN_STEP(HistoryStep)
    RETURN_STEP(ChangeStep)
    RETURN_STEP(CreateNodeStep)
    RETURN_STEP(CreateEdgeStep)
    RETURN_STEP(CommitStep)
    RETURN_STEP(CallPropertyStep)
    RETURN_STEP(CallLabelStep)
    RETURN_STEP(CallEdgeTypeStep)
    RETURN_STEP(CallLabelSetStep)

// Exit execution
ExecutorExit:
    pipeStep++;
}

void Executor::checkJumpTables() {
    for (uint64_t i = 0; i < (uint64_t)PipelineOpcode::MAX; i++) {
        if (_activateTbl[i] == nullptr) {
            throw PipelineException("Activate jump table is not initialized at location " + std::to_string(i));
        }
        if (_returnTbl[i] == nullptr) {
            throw PipelineException("Return jump table is not initialized at location " + std::to_string(i));
        }
    }
}
