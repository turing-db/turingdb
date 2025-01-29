#include "Executor.h"

#include <spdlog/spdlog.h>

#include "Pipeline.h"
#include "PipelineException.h"
#include "PipelineMacros.h"
#include "PipelineOpcode.h"
#include "Panic.h"

using namespace db;

using PipelineJumpTable = std::array<void*, (uint64_t)PipelineOpcode::MAX>;

// Global definition of jump tables
// to not create them during each call to Executor::run()
namespace {

PipelineJumpTable activateTbl;
PipelineJumpTable returnTbl;

constexpr bool checkJumpTables() {
    for (uint64_t i = 0; i < (uint64_t)PipelineOpcode::MAX; i++) {
        if (activateTbl[i] == 0) {
            return false;
        }
        if (returnTbl[i] == 0) {
            return false;
        }
    }

    return true;
}

}

bool Executor::run(bool initRun) {
    try {
        runImpl(initRun);
        return true;
    } catch (const PipelineException&) {
        return false;
    }
}

void Executor::runImpl(bool initRun) {
    if (initRun) {
        activateTbl.fill(nullptr);
        returnTbl.fill(nullptr);

        // ACTIVATE jump table
        activateTbl[(uint64_t)PipelineOpcode::STOP] = GOTOPTR(StopStep);
        activateTbl[(uint64_t)PipelineOpcode::SCAN_NODES] = ACTIVATE_PTR(ScanNodesStep);
        activateTbl[(uint64_t)PipelineOpcode::SCAN_NODES_BY_LABEL] = ACTIVATE_PTR(ScanNodesByLabelStep);
        activateTbl[(uint64_t)PipelineOpcode::SCAN_EDGES] = ACTIVATE_PTR(ScanEdgesStep);
        activateTbl[(uint64_t)PipelineOpcode::SCAN_IN_EDGES_BY_LABEL] = ACTIVATE_PTR(ScanInEdgesByLabelStep);
        activateTbl[(uint64_t)PipelineOpcode::SCAN_OUT_EDGES_BY_LABEL] = ACTIVATE_PTR(ScanOutEdgesByLabelStep);
        activateTbl[(uint64_t)PipelineOpcode::GET_OUT_EDGES] = ACTIVATE_PTR(GetOutEdgesStep);
        activateTbl[(uint64_t)PipelineOpcode::GET_LABELSETID_STEP] = ACTIVATE_PTR(GetLabelSetIDStep);
        activateTbl[(uint64_t)PipelineOpcode::FILTER] = ACTIVATE_PTR(FilterStep);
        activateTbl[(uint64_t)PipelineOpcode::TRANSFORM] = ACTIVATE_PTR(TransformStep);
        activateTbl[(uint64_t)PipelineOpcode::COUNT] = ACTIVATE_PTR(CountStep);
        activateTbl[(uint64_t)PipelineOpcode::LAMBDA] = ACTIVATE_PTR(LambdaStep);
        activateTbl[(uint64_t)PipelineOpcode::JSON_ENCODER] = ACTIVATE_PTR(JsonEncoderStep);
        activateTbl[(uint64_t)PipelineOpcode::DEBUG_DUMP] = ACTIVATE_PTR(DebugDumpStep);
        activateTbl[(uint64_t)PipelineOpcode::CREATE_GRAPH] = ACTIVATE_PTR(CreateGraphStep);
        activateTbl[(uint64_t)PipelineOpcode::LIST_GRAPH] = ACTIVATE_PTR(ListGraphStep);
        activateTbl[(uint64_t)PipelineOpcode::LOAD_GRAPH] = ACTIVATE_PTR(LoadGraphStep);
        activateTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_INT64] = ACTIVATE_PTR(GetPropertyInt64Step);
        activateTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_UINT64] = ACTIVATE_PTR(GetPropertyUInt64Step);
        activateTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_DOUBLE] = ACTIVATE_PTR(GetPropertyDoubleStep);
        activateTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_STRING] = ACTIVATE_PTR(GetPropertyStringStep);
        activateTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_BOOL] = ACTIVATE_PTR(GetPropertyBoolStep);
        activateTbl[(uint64_t)PipelineOpcode::END] = ACTIVATE_END_PTR();

        // RETURN jump table
        returnTbl[(uint64_t)PipelineOpcode::STOP] = GOTOPTR(StopStep);
        returnTbl[(uint64_t)PipelineOpcode::SCAN_NODES] = RETURN_PTR(ScanNodesStep);
        returnTbl[(uint64_t)PipelineOpcode::SCAN_NODES_BY_LABEL] = RETURN_PTR(ScanNodesByLabelStep);
        returnTbl[(uint64_t)PipelineOpcode::SCAN_EDGES] = RETURN_PTR(ScanEdgesStep);
        returnTbl[(uint64_t)PipelineOpcode::SCAN_IN_EDGES_BY_LABEL] = RETURN_PTR(ScanInEdgesByLabelStep);
        returnTbl[(uint64_t)PipelineOpcode::SCAN_OUT_EDGES_BY_LABEL] = RETURN_PTR(ScanOutEdgesByLabelStep);
        returnTbl[(uint64_t)PipelineOpcode::GET_OUT_EDGES] = RETURN_PTR(GetOutEdgesStep);
        returnTbl[(uint64_t)PipelineOpcode::GET_LABELSETID_STEP] = RETURN_PTR(GetLabelSetIDStep);
        returnTbl[(uint64_t)PipelineOpcode::FILTER] = RETURN_PTR(FilterStep);
        returnTbl[(uint64_t)PipelineOpcode::TRANSFORM] = RETURN_PTR(TransformStep);
        returnTbl[(uint64_t)PipelineOpcode::COUNT] = RETURN_PTR(CountStep);
        returnTbl[(uint64_t)PipelineOpcode::LAMBDA] = RETURN_PTR(LambdaStep);
        returnTbl[(uint64_t)PipelineOpcode::JSON_ENCODER] = RETURN_PTR(JsonEncoderStep);
        returnTbl[(uint64_t)PipelineOpcode::DEBUG_DUMP] = RETURN_PTR(DebugDumpStep);
        returnTbl[(uint64_t)PipelineOpcode::CREATE_GRAPH] = RETURN_PTR(CreateGraphStep);
        returnTbl[(uint64_t)PipelineOpcode::LIST_GRAPH] = RETURN_PTR(ListGraphStep);
        returnTbl[(uint64_t)PipelineOpcode::LOAD_GRAPH] = RETURN_PTR(LoadGraphStep);
        returnTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_INT64] = RETURN_PTR(GetPropertyInt64Step);
        returnTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_UINT64] = RETURN_PTR(GetPropertyUInt64Step);
        returnTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_DOUBLE] = RETURN_PTR(GetPropertyDoubleStep);
        returnTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_STRING] = RETURN_PTR(GetPropertyStringStep);
        returnTbl[(uint64_t)PipelineOpcode::GET_PROPERTY_BOOL] = RETURN_PTR(GetPropertyBoolStep);
        returnTbl[(uint64_t)PipelineOpcode::END] = GOTOPTR(StopStep);

        if (!checkJumpTables()) {
            panic("Jump tables not correctly initialized in Executor");
        }

        return;
    }

    const auto steps = _pipeline->steps();

    // Check that the pipeline has at least a size of 2 steps (needs STOP and END)
    if (steps.size() < 2) {
        spdlog::error("Pipeline size is less than 2");
        return;
    }

    // Check that the first step is a STOP step
    if (steps.front().getOpcode() != PipelineOpcode::STOP) {
        spdlog::error("Pipeline has no STOP step");
        return;
    }

    // Check that the last step is an END step
    if (steps.back().getOpcode() != PipelineOpcode::END) {
        spdlog::error("Pipeline has no END step");
        return;
    }

    // Prepare steps
    prepare();

    PipelineStep* pipeStep = steps.data();

    // Go to first step
    pipeStep++;
    goto *activateTbl[(uint64_t)pipeStep->getOpcode()];

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
    ACTIVATE_STEP(JsonEncoderStep)
    ACTIVATE_STEP(DebugDumpStep)
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
    RETURN_STEP(JsonEncoderStep)
    RETURN_STEP(CreateGraphStep)
    RETURN_STEP(ListGraphStep)
    RETURN_STEP(LoadGraphStep)
    RETURN_STEP(DebugDumpStep)
    RETURN_STEP(GetPropertyInt64Step)
    RETURN_STEP(GetPropertyUInt64Step)
    RETURN_STEP(GetPropertyDoubleStep)
    RETURN_STEP(GetPropertyStringStep)
    RETURN_STEP(GetPropertyBoolStep)

    // Exit execution
    ExecutorExit:
    pipeStep++;
}

void Executor::prepare() {
    for (PipelineStep& step : _pipeline->steps()) {
        step.prepare(_ctxt);
    }
}

void Executor::init() {
    Executor executor(nullptr, nullptr);
    executor.run(true);
}
