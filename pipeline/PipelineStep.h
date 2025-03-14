#pragma once

#include <variant>

#include "PipelineOpcode.h"

#include "operations/ScanNodesStep.h"
#include "operations/ScanNodesByLabelStep.h"
#include "operations/ScanEdgesStep.h"
#include "operations/ScanInEdgesByLabelStep.h"
#include "operations/ScanOutEdgesByLabelStep.h"
#include "operations/GetOutEdgesStep.h"
#include "operations/TransformStep.h"
#include "operations/CountStep.h"
#include "operations/LambdaStep.h"
#include "operations/StopStep.h"
#include "operations/EndStep.h"
#include "operations/FilterStep.h"
#include "operations/CreateGraphStep.h"
#include "operations/ListGraphStep.h"
#include "operations/GetLabelSetIDStep.h"
#include "operations/LoadGraphStep.h"
#include "operations/GetPropertyStep.h"

#include "FastGet.h"

#define GET_PROPERTY_STEP(TYPE)                                        \
    PipelineStep(GetNodeProperty##TYPE##Step::Tag,                     \
                 const ColumnIDs* entityIDs,                           \
                 PropertyType propertyType,                            \
                 ColumnOptVector<types::TYPE::Primitive>* propValues); \
    PipelineStep(GetEdgeProperty##TYPE##Step::Tag,                     \
                 const ColumnIDs* entityIDs,                           \
                 PropertyType propertyType,                            \
                 ColumnOptVector<types::TYPE::Primitive>* propValues); \


namespace net {
    class NetWriter;
}

namespace db {

struct EdgeWriteInfo;

class PipelineStep {
public:
    PipelineStep(ScanNodesStep::Tag, ColumnIDs* nodes);
    PipelineStep(ScanNodesByLabelStep::Tag,
                 ColumnIDs* nodes,
                 const LabelSet* labelSet);
    PipelineStep(ScanEdgesStep::Tag,
                 const EdgeWriteInfo& edgeWriteInfo);
    PipelineStep(ScanInEdgesByLabelStep::Tag,
                 const EdgeWriteInfo& edgeWriteInfo,
                 const LabelSet* labelSet);
    PipelineStep(ScanOutEdgesByLabelStep::Tag,
                 const EdgeWriteInfo& edgeWriteInfo,
                 const LabelSet* labelSet);
    PipelineStep(GetOutEdgesStep::Tag,
                 const ColumnIDs* inputNodeIDs,
                 const EdgeWriteInfo& edgeWriteInfo);
    PipelineStep(GetLabelSetIDStep::Tag,
                 const ColumnIDs* nodeIDs,
                 ColumnVector<LabelSetID>* labelsetIDs);
    PipelineStep(FilterStep::Tag,
                 ColumnVector<size_t>* indices);
    PipelineStep(TransformStep::Tag,
                 TransformData* transformData);
    PipelineStep(CountStep::Tag,
                 const Column* input,
                 CountStep::ColumnCount* count);
    PipelineStep(LambdaStep::Tag,
                 LambdaStep::StepFunc stepFunc);
    PipelineStep(CreateGraphStep::Tag,
                 const std::string& graphName);
    PipelineStep(ListGraphStep::Tag,
                 ColumnVector<std::string_view>* graphNames);
    PipelineStep(StopStep::Tag);
    PipelineStep(EndStep::Tag);
    PipelineStep(LoadGraphStep::Tag, const std::string& graphName);

    GET_PROPERTY_STEP(Int64)
    GET_PROPERTY_STEP(UInt64)
    GET_PROPERTY_STEP(Double)
    GET_PROPERTY_STEP(String)
    GET_PROPERTY_STEP(Bool)

    PipelineStep(PipelineStep&& other) = default;

    ~PipelineStep();

    inline PipelineOpcode getOpcode() const { return _opcode; }

    template <typename StepT>
    inline StepT& get() { return FastGet<StepT>(_impl); }

    void prepare(ExecutionContext* ctxt) {
        std::visit([ctxt](auto& step) { step.prepare(ctxt); }, _impl);
    }

    void describe(std::string& descr) const;

private:
    PipelineOpcode _opcode;

    std::variant<ScanNodesStep,
                 ScanNodesByLabelStep,
                 ScanEdgesStep,
                 ScanInEdgesByLabelStep,
                 ScanOutEdgesByLabelStep,
                 GetOutEdgesStep,
                 GetLabelSetIDStep,
                 FilterStep,
                 TransformStep,
                 CountStep,
                 LambdaStep,
                 StopStep,
                 EndStep,
                 CreateGraphStep,
                 ListGraphStep,
                 LoadGraphStep,
                 GetNodePropertyInt64Step,
                 GetNodePropertyUInt64Step,
                 GetNodePropertyDoubleStep,
                 GetNodePropertyStringStep,
                 GetNodePropertyBoolStep,
                 GetEdgePropertyInt64Step,
                 GetEdgePropertyUInt64Step,
                 GetEdgePropertyDoubleStep,
                 GetEdgePropertyStringStep,
                 GetEdgePropertyBoolStep> _impl;
};

}
