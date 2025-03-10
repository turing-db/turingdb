#include "Executor.h"

#include "Pipeline.h"
#include "PipelineException.h"
#include "PipelineMacros.h"

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
        _activateTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_INT64] = ACTIVATE_PTR(GetPropertyInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_UINT64] = ACTIVATE_PTR(GetPropertyUInt64Step);
        _activateTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_DOUBLE] = ACTIVATE_PTR(GetPropertyDoubleStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_STRING] = ACTIVATE_PTR(GetPropertyStringStep);
        _activateTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_BOOL] = ACTIVATE_PTR(GetPropertyBoolStep);
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
        _returnTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_INT64] = RETURN_PTR(GetPropertyInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_UINT64] = RETURN_PTR(GetPropertyUInt64Step);
        _returnTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_DOUBLE] = RETURN_PTR(GetPropertyDoubleStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_STRING] = RETURN_PTR(GetPropertyStringStep);
        _returnTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_BOOL] = RETURN_PTR(GetPropertyBoolStep);
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
    ACTIVATE_STEP(GetPropertyInt64Step)
    ACTIVATE_STEP(GetPropertyUInt64Step)
    ACTIVATE_STEP(GetPropertyDoubleStep)
    ACTIVATE_STEP(GetPropertyStringStep)
    ACTIVATE_STEP(GetPropertyBoolStep)
    ACTIVATE_END()

    // RETURN actions
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
    RETURN_STEP(GetPropertyInt64Step)
    RETURN_STEP(GetPropertyUInt64Step)
    RETURN_STEP(GetPropertyDoubleStep)
    RETURN_STEP(GetPropertyStringStep)
    RETURN_STEP(GetPropertyBoolStep)

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
