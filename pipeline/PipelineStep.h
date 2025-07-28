#pragma once

#include <variant>

#include "ChangeOpType.h"
#include "CallEdgeTypeStep.h"
#include "CallLabelSetStep.h"
#include "CallLabelStep.h"
#include "PipelineOpcode.h"

#include "ScanNodesStringApproxStep.h"
#include "operations/ScanNodesStep.h"
#include "operations/ScanNodesByLabelStep.h"
#include "operations/ScanNodesByPropertyStep.h"
#include "operations/ScanNodesByPropertyAndLabelStep.h"
#include "operations/ScanEdgesStep.h"
#include "operations/ScanInEdgesByLabelStep.h"
#include "operations/ScanOutEdgesByLabelStep.h"
#include "operations/ScanNodesStringApproxStep.h"
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
#include "operations/QueryIndexStep.h"
#include "operations/GetPropertyStep.h"
#include "operations/GetFilteredPropertyStep.h"
#include "operations/HistoryStep.h"
#include "operations/ChangeStep.h"
#include "operations/CreateNodeStep.h"
#include "operations/CreateEdgeStep.h"
#include "operations/CommitStep.h"
#include "operations/CallPropertyStep.h"

#include "FastGet.h"

#define PROPERTY_STEPS(TYPE)                                           \
    PipelineStep(GetNodeProperty##TYPE##Step::Tag,                     \
                 const ColumnNodeIDs* entityIDs,                       \
                 PropertyType propertyType,                            \
                 ColumnOptVector<types::TYPE::Primitive>* propValues); \
    PipelineStep(GetEdgeProperty##TYPE##Step::Tag,                     \
                 const ColumnEdgeIDs* entityIDs,                       \
                 PropertyType propertyType,                            \
                 ColumnOptVector<types::TYPE::Primitive>* propValues); \
    PipelineStep(ScanNodesByProperty##TYPE##Step::Tag,                 \
                 ColumnNodeIDs* nodeIDs,                               \
                 PropertyType propertyType,                            \
                 ColumnVector<types::TYPE::Primitive>* propValues);    \
    PipelineStep(ScanNodesByPropertyAndLabel##TYPE##Step::Tag,         \
                 ColumnNodeIDs* nodeIDs,                               \
                 PropertyType propertyType,                            \
                 const LabelSetHandle& labelSet,                       \
                 ColumnVector<types::TYPE::Primitive>* propValues);    \
    PipelineStep(GetFilteredNodeProperty##TYPE##Step::Tag,             \
                 const ColumnNodeIDs* entityIDs,                       \
                 PropertyType propertyType,                            \
                 ColumnVector<types::TYPE::Primitive>* propValues,     \
                 ColumnVector<size_t>* indices,                        \
                 ColumnMask* projectedMask);                           \
    PipelineStep(GetFilteredEdgeProperty##TYPE##Step::Tag,             \
                 const ColumnEdgeIDs* entityIDs,                       \
                 PropertyType propertyType,                            \
                 ColumnVector<types::TYPE::Primitive>* propValues,     \
                 ColumnVector<size_t>* indices,                        \
                 ColumnMask* projectedMask);

namespace net {
class NetWriter;
}

namespace db {

struct EdgeWriteInfo;

class PipelineStep {
public:

    PipelineStep(ScanNodesStringApproxStep::Tag, ColumnVector<NodeID>* nodes, const GraphView& view,
                           PropertyTypeID propID, std::string_view strQuery);
    PipelineStep(ScanNodesStep::Tag, ColumnNodeIDs* nodes);
    PipelineStep(ScanNodesByLabelStep::Tag,
                 ColumnNodeIDs* nodes,
                 const LabelSetHandle& labelSet);
    PipelineStep(ScanEdgesStep::Tag,
                 const EdgeWriteInfo& edgeWriteInfo);
    PipelineStep(ScanInEdgesByLabelStep::Tag,
                 const EdgeWriteInfo& edgeWriteInfo,
                 const LabelSetHandle& labelSet);
    PipelineStep(ScanOutEdgesByLabelStep::Tag,
                 const EdgeWriteInfo& edgeWriteInfo,
                 const LabelSetHandle& labelSet);
    PipelineStep(GetOutEdgesStep::Tag,
                 const ColumnNodeIDs* inputNodeIDs,
                 const EdgeWriteInfo& edgeWriteInfo);
    PipelineStep(GetLabelSetIDStep::Tag,
                 const ColumnNodeIDs* nodeIDs,
                 ColumnVector<LabelSetID>* labelsetIDs);
    PipelineStep(FilterStep::Tag,
                 ColumnVector<size_t>* indices);
    PipelineStep(FilterStep::Tag);
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
    PipelineStep(HistoryStep::Tag, ColumnVector<std::string>*);
    PipelineStep(ChangeStep::Tag,
                 ChangeOpType,
                 ColumnVector<const Change*>*);
    PipelineStep(LoadGraphStep::Tag, const std::string& graphName);

    PipelineStep(QueryNodeIndexStep::Tag, ColumnSet<NodeID>* outSet,
                 const GraphView& view, PropertyTypeID propID,
                 const std::string& strQuery);

    PipelineStep(QueryEdgeIndexStep::Tag, ColumnSet<EdgeID>* outSet,
                 const GraphView& view, PropertyTypeID propID,
                 const std::string& strQuery);

    PipelineStep(CreateNodeStep::Tag, const EntityPattern*);
    PipelineStep(CreateEdgeStep::Tag,
                 const EntityPattern*,
                 const EntityPattern*,
                 const EntityPattern*);
    PipelineStep(CommitStep::Tag);
    PipelineStep(CallPropertyStep::Tag,
                 ColumnVector<PropertyTypeID>* id,
                 ColumnVector<std::string_view>* name,
                 ColumnVector<std::string_view>* type);
    PipelineStep(CallLabelStep::Tag,
                 ColumnVector<LabelID>* id,
                 ColumnVector<std::string_view>* name);
    PipelineStep(CallEdgeTypeStep::Tag,
                 ColumnVector<EdgeTypeID>* id,
                 ColumnVector<std::string_view>* name);
    PipelineStep(CallLabelSetStep::Tag,
                 ColumnVector<LabelSetID>* id,
                 ColumnVector<std::string_view>* name);

    PROPERTY_STEPS(Int64)
    PROPERTY_STEPS(UInt64)
    PROPERTY_STEPS(Double)
    PROPERTY_STEPS(String)
    PROPERTY_STEPS(Bool)

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
                 QueryNodeIndexStep,
                 QueryEdgeIndexStep,
                 ScanNodesByPropertyInt64Step,
                 ScanNodesByPropertyUInt64Step,
                 ScanNodesByPropertyDoubleStep,
                 ScanNodesByPropertyStringStep,
                 ScanNodesStringApproxStep,
                 ScanNodesByPropertyBoolStep,
                 ScanNodesByPropertyAndLabelInt64Step,
                 ScanNodesByPropertyAndLabelUInt64Step,
                 ScanNodesByPropertyAndLabelDoubleStep,
                 ScanNodesByPropertyAndLabelStringStep,
                 ScanNodesByPropertyAndLabelBoolStep,
                 GetNodePropertyInt64Step,
                 GetNodePropertyUInt64Step,
                 GetNodePropertyDoubleStep,
                 GetNodePropertyStringStep,
                 GetNodePropertyBoolStep,
                 GetEdgePropertyInt64Step,
                 GetEdgePropertyUInt64Step,
                 GetEdgePropertyDoubleStep,
                 GetEdgePropertyStringStep,
                 GetEdgePropertyBoolStep,
                 GetFilteredNodePropertyInt64Step,
                 GetFilteredNodePropertyUInt64Step,
                 GetFilteredNodePropertyDoubleStep,
                 GetFilteredNodePropertyStringStep,
                 GetFilteredNodePropertyBoolStep,
                 GetFilteredEdgePropertyInt64Step,
                 GetFilteredEdgePropertyUInt64Step,
                 GetFilteredEdgePropertyDoubleStep,
                 GetFilteredEdgePropertyStringStep,
                 GetFilteredEdgePropertyBoolStep,
                 HistoryStep,
                 ChangeStep,
                 CreateNodeStep,
                 CreateEdgeStep,
                 CommitStep,
                 CallPropertyStep,
                 CallLabelStep,
                 CallLabelSetStep,
                 CallEdgeTypeStep>
        _impl;
};

}
