#include "PipelineStep.h"

#define GET_PROPERTY_STEP_IMPL(NodeOrEdge, Opcode, Type)                            \
    PipelineStep::PipelineStep(Get##NodeOrEdge##Property##Type##Step::Tag,          \
                               const ColumnIDs* entityIDs,                          \
                               PropertyType propertyType,                           \
                               ColumnOptVector<types::Type::Primitive>* propValues) \
        : _opcode(PipelineOpcode::Opcode),                                          \
        _impl(std::in_place_type<Get##NodeOrEdge##Property##Type##Step>,            \
              entityIDs,                                                            \
              propertyType,                                                         \
              propValues)                                                           \
    {                                                                               \
    }                                                                               \


using namespace db;

PipelineStep::PipelineStep(ScanNodesStep::Tag, ColumnIDs* nodes)
    : _opcode(PipelineOpcode::SCAN_NODES),
    _impl(std::in_place_type<ScanNodesStep>, nodes)
{
}

PipelineStep::PipelineStep(ScanEdgesStep::Tag,
                           const EdgeWriteInfo& edgeWriteInfo)
    : _opcode(PipelineOpcode::SCAN_EDGES),
    _impl(std::in_place_type<ScanEdgesStep>, edgeWriteInfo)
{
}

PipelineStep::PipelineStep(ScanInEdgesByLabelStep::Tag,
                           const EdgeWriteInfo& edgeWriteInfo,
                           const LabelSet* labelSet)
    : _opcode(PipelineOpcode::SCAN_IN_EDGES_BY_LABEL),
    _impl(std::in_place_type<ScanInEdgesByLabelStep>, edgeWriteInfo, labelSet)
{
}

PipelineStep::PipelineStep(ScanOutEdgesByLabelStep::Tag,
                           const EdgeWriteInfo& edgeWriteInfo,
                           const LabelSet* labelSet)
    : _opcode(PipelineOpcode::SCAN_OUT_EDGES_BY_LABEL),
    _impl(std::in_place_type<ScanOutEdgesByLabelStep>, edgeWriteInfo, labelSet)
{
}

PipelineStep::PipelineStep(GetOutEdgesStep::Tag,
                           const ColumnIDs* inputNodeIDs,
                           const EdgeWriteInfo& edgeWriteInfo)
    : _opcode(PipelineOpcode::GET_OUT_EDGES),
    _impl(std::in_place_type<GetOutEdgesStep>, inputNodeIDs, edgeWriteInfo)
{
}

PipelineStep::PipelineStep(ScanNodesByLabelStep::Tag,
                           ColumnIDs* nodes,
                           const LabelSet* labelSet)
    : _opcode(PipelineOpcode::SCAN_NODES_BY_LABEL),
    _impl(std::in_place_type<ScanNodesByLabelStep>, nodes, labelSet)
{
}

PipelineStep::PipelineStep(FilterStep::Tag,
                           ColumnVector<size_t>* indices)
    : _opcode(PipelineOpcode::FILTER),
    _impl(std::in_place_type<FilterStep>, indices)
{
}

PipelineStep::PipelineStep(TransformStep::Tag,
                           TransformData* transformData)
    : _opcode(PipelineOpcode::TRANSFORM),
    _impl(std::in_place_type<TransformStep>, transformData)
{
}

PipelineStep::PipelineStep(LambdaStep::Tag,
                           LambdaStep::StepFunc stepFunc)
    : _opcode(PipelineOpcode::LAMBDA),
    _impl(std::in_place_type<LambdaStep>, stepFunc)
{
}

PipelineStep::PipelineStep(CountStep::Tag,
                           const Column* input,
                           CountStep::ColumnCount* count)
    : _opcode(PipelineOpcode::COUNT),
    _impl(std::in_place_type<CountStep>, input, count)
{
}

PipelineStep::PipelineStep(StopStep::Tag)
    : _opcode(PipelineOpcode::STOP),
    _impl(std::in_place_type<StopStep>)
{
}

PipelineStep::PipelineStep(EndStep::Tag)
    : _opcode(PipelineOpcode::END),
    _impl(std::in_place_type<EndStep>)
{
}

PipelineStep::PipelineStep(CreateGraphStep::Tag, const std::string& graphName)
    : _opcode(PipelineOpcode::CREATE_GRAPH),
    _impl(std::in_place_type<CreateGraphStep>, graphName)
{
}

PipelineStep::PipelineStep(ListGraphStep::Tag, ColumnVector<std::string_view>* graphNames)
    : _opcode(PipelineOpcode::LIST_GRAPH),
    _impl(std::in_place_type<ListGraphStep>, graphNames)
{
}

PipelineStep::PipelineStep(GetLabelSetIDStep::Tag,
                           const ColumnIDs* nodeIDs,
                           ColumnVector<LabelSetID>* labelsetIDs)
    : _opcode(PipelineOpcode::GET_LABELSETID_STEP),
    _impl(std::in_place_type<GetLabelSetIDStep>, nodeIDs, labelsetIDs)
{
}

PipelineStep::PipelineStep(LoadGraphStep::Tag, const std::string& graphName)
    : _opcode(PipelineOpcode::LOAD_GRAPH),
    _impl(std::in_place_type<LoadGraphStep>, graphName)
{
}

GET_PROPERTY_STEP_IMPL(Node, GET_NODE_PROPERTY_INT64, Int64)
GET_PROPERTY_STEP_IMPL(Node, GET_NODE_PROPERTY_UINT64, UInt64)
GET_PROPERTY_STEP_IMPL(Node, GET_NODE_PROPERTY_DOUBLE, Double)
GET_PROPERTY_STEP_IMPL(Node, GET_NODE_PROPERTY_STRING, String)
GET_PROPERTY_STEP_IMPL(Node, GET_NODE_PROPERTY_BOOL, Bool)

GET_PROPERTY_STEP_IMPL(Edge, GET_EDGE_PROPERTY_INT64, Int64)
GET_PROPERTY_STEP_IMPL(Edge, GET_EDGE_PROPERTY_UINT64, UInt64)
GET_PROPERTY_STEP_IMPL(Edge, GET_EDGE_PROPERTY_DOUBLE, Double)
GET_PROPERTY_STEP_IMPL(Edge, GET_EDGE_PROPERTY_STRING, String)
GET_PROPERTY_STEP_IMPL(Edge, GET_EDGE_PROPERTY_BOOL, Bool)

PipelineStep::~PipelineStep() {
}

void PipelineStep::describe(std::string& descr) const {
    std::visit([&](auto& step) { step.describe(descr); }, _impl);
}
