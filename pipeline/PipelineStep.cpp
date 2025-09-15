#include "PipelineStep.h"
#include "ID.h"
#include "PipelineOpcode.h"

#define GET_PROPERTY_STEP_IMPL(NodeOrEdge, Opcode, Type)                             \
    PipelineStep::PipelineStep(Get##NodeOrEdge##Property##Type##Step::Tag,           \
                               const Column##NodeOrEdge##IDs* entityIDs,             \
                               PropertyType propertyType,                            \
                               ColumnOptVector<types::Type::Primitive>* propValues)  \
        : _opcode(PipelineOpcode::Opcode),                                           \
        _impl(std::in_place_type<Get##NodeOrEdge##Property##Type##Step>,             \
              entityIDs,                                                             \
              propertyType,                                                          \
              propValues)                                                            \
    {                                                                                \
    }

#define SCAN_NODE_PROPERTIES_IMPL(Opcode, Type)                                  \
    PipelineStep::PipelineStep(ScanNodesByProperty##Type##Step::Tag,             \
                               ColumnNodeIDs* nodeIDs,                           \
                               PropertyType propertyType,                        \
                               ColumnVector<types::Type::Primitive>* propValues) \
        : _opcode(PipelineOpcode::Opcode),                                       \
          _impl(std::in_place_type<ScanNodesByProperty##Type##Step>,             \
                nodeIDs,                                                         \
                propertyType,                                                    \
                propValues)                                                      \
    {                                                                            \
    }                                                                            \

#define SCAN_NODE_PROPERTIES_AND_LABEL_IMPL(Opcode, Type)                        \
    PipelineStep::PipelineStep(ScanNodesByPropertyAndLabel##Type##Step::Tag,     \
                               ColumnNodeIDs* nodeIDs,                           \
                               PropertyType propertyType,                        \
                               const LabelSetHandle& labelSet,                   \
                               ColumnVector<types::Type::Primitive>* propValues) \
        : _opcode(PipelineOpcode::Opcode),                                       \
          _impl(std::in_place_type<ScanNodesByPropertyAndLabel##Type##Step>,     \
                nodeIDs,                                                         \
                propertyType,                                                    \
                labelSet,                                                        \
                propValues)                                                      \
    {                                                                            \
    }                                                                            \

#define GET_FILTERED_PROPERTY_STEP_IMPL(NodeOrEdge, Opcode, Type)                  \
    PipelineStep::PipelineStep(GetFiltered##NodeOrEdge##Property##Type##Step::Tag, \
                               const Column##NodeOrEdge##IDs* entityIDs,           \
                               PropertyType propertyType,                          \
                               ColumnVector<types::Type::Primitive>* propValues,   \
                               ColumnVector<size_t>* indices,                      \
                               ColumnMask* projectedMask)                          \
        : _opcode(PipelineOpcode::Opcode),                                         \
          _impl(std::in_place_type<GetFiltered##NodeOrEdge##Property##Type##Step>, \
                entityIDs,                                                         \
                propertyType,                                                      \
                propValues,                                                        \
                indices,                                                           \
                projectedMask)                                                     \
    {                                                                              \
    }                                                                              \


using namespace db;

PipelineStep::PipelineStep(ScanNodesStringApproxStep::Tag, ColumnVector<NodeID>* nodes,
                           const GraphView& view, PropertyTypeID propID, std::string_view strQuery)
    : _opcode(PipelineOpcode::SCAN_NODE_PROPERTY_STRING_APPROX),
    _impl(std::in_place_type<ScanNodesStringApproxStep>, nodes, view, propID, strQuery)
{
}

PipelineStep::PipelineStep(ScanNodesStep::Tag, ColumnNodeIDs* nodes)
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
                           const LabelSetHandle& labelSet)
    : _opcode(PipelineOpcode::SCAN_IN_EDGES_BY_LABEL),
    _impl(std::in_place_type<ScanInEdgesByLabelStep>, edgeWriteInfo, labelSet)
{
}

PipelineStep::PipelineStep(ScanOutEdgesByLabelStep::Tag,
                           const EdgeWriteInfo& edgeWriteInfo,
                           const LabelSetHandle& labelSet)
    : _opcode(PipelineOpcode::SCAN_OUT_EDGES_BY_LABEL),
    _impl(std::in_place_type<ScanOutEdgesByLabelStep>, edgeWriteInfo, labelSet)
{
}

PipelineStep::PipelineStep(GetOutEdgesStep::Tag,
                           const ColumnNodeIDs* inputNodeIDs,
                           const EdgeWriteInfo& edgeWriteInfo)
    : _opcode(PipelineOpcode::GET_OUT_EDGES),
    _impl(std::in_place_type<GetOutEdgesStep>, inputNodeIDs, edgeWriteInfo)
{
}

PipelineStep::PipelineStep(ScanNodesByLabelStep::Tag,
                           ColumnNodeIDs* nodes,
                           const LabelSetHandle& labelSet)
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

PipelineStep::PipelineStep(FilterStep::Tag)
    : _opcode(PipelineOpcode::FILTER),
    _impl(std::in_place_type<FilterStep>)
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
                           const ColumnNodeIDs* nodeIDs,
                           ColumnVector<LabelSetID>* labelsetIDs)
    : _opcode(PipelineOpcode::GET_LABELSETID_STEP),
    _impl(std::in_place_type<GetLabelSetIDStep>, nodeIDs, labelsetIDs)
{
}

PipelineStep::PipelineStep(HistoryStep::Tag,
                           ColumnVector<std::string>* log)
    : _opcode(PipelineOpcode::HISTORY),
    _impl(std::in_place_type<HistoryStep>, log)
{
}

PipelineStep::PipelineStep(ChangeStep::Tag,
                           ChangeOpType type,
                           ColumnVector<const Change*>* changes)
    : _opcode(PipelineOpcode::CHANGE),
    _impl(std::in_place_type<ChangeStep>, type, changes)
{
}

PipelineStep::PipelineStep(CallPropertyStep::Tag,
             ColumnVector<PropertyTypeID>* id,
             ColumnVector<std::string_view>* name,
             ColumnVector<std::string_view>* type)
    :_opcode(PipelineOpcode::CALL_PROPERTIES),
    _impl(std::in_place_type<CallPropertyStep>, id, name, type)
{
}

PipelineStep::PipelineStep(CallLabelStep::Tag,
             ColumnVector<LabelID>* id,
             ColumnVector<std::string_view>* name)
    :_opcode(PipelineOpcode::CALL_LABELS),
    _impl(std::in_place_type<CallLabelStep>, id, name)
{
}

PipelineStep::PipelineStep(CallEdgeTypeStep::Tag,
             ColumnVector<EdgeTypeID>* id,
             ColumnVector<std::string_view>* name)
    :_opcode(PipelineOpcode::CALL_EDGETYPES),
    _impl(std::in_place_type<CallEdgeTypeStep>, id, name)
{
}

PipelineStep::PipelineStep(CallLabelSetStep::Tag,
             ColumnVector<LabelSetID>* id,
             ColumnVector<std::string_view>* name)
    :_opcode(PipelineOpcode::CALL_LABELSETS),
    _impl(std::in_place_type<CallLabelSetStep>, id, name)
{
}

PipelineStep::PipelineStep(LoadGraphStep::Tag, const std::string& graphFileName)
    : _opcode(PipelineOpcode::LOAD_GRAPH),
    _impl(std::in_place_type<LoadGraphStep>, graphFileName)
{
}

PipelineStep::PipelineStep(LoadGraphStep::Tag, const std::string& graphFileName, const std::string& graphName)
    : _opcode(PipelineOpcode::LOAD_GRAPH),
    _impl(std::in_place_type<LoadGraphStep>, graphFileName, graphName)
{
}

PipelineStep::PipelineStep(LookupNodeIndexStep::Tag, ColumnSet<NodeID>* outSet,
                           const GraphView& view, PropertyTypeID propID,
                           const std::string& strQuery)
    : _opcode(PipelineOpcode::QUERY_NODE_INDEX),
    _impl(std::in_place_type<LookupNodeIndexStep>, outSet, view, propID, strQuery)
{
}

PipelineStep::PipelineStep(LookupEdgeIndexStep::Tag, ColumnSet<EdgeID>* outSet,
                           const GraphView& view, PropertyTypeID propID,
                           const std::string& strQuery)
    : _opcode(PipelineOpcode::QUERY_EDGE_INDEX),
    _impl(std::in_place_type<LookupEdgeIndexStep>, outSet, view, propID, strQuery)
{
}

PipelineStep::PipelineStep(CreateNodeStep::Tag, const EntityPattern* data)
    : _opcode(PipelineOpcode::CREATE_NODE),
    _impl(std::in_place_type<CreateNodeStep>, data)
{
}

PipelineStep::PipelineStep(S3ConnectStep::Tag,
                           const std::string& accessId,
                           const std::string& secretKey,
                           const std::string& region)
    :_opcode(PipelineOpcode::S3CONNECT),
    _impl(std::in_place_type<S3ConnectStep>, accessId, secretKey, region)
{
}
PipelineStep::PipelineStep(S3PushStep::Tag,
                           std::string_view s3Bucket,
                           std::string_view s3Prefix,
                           std::string_view s3File,
                           const std::string& localPath)
    :_opcode(PipelineOpcode::S3PUSH),
    _impl(std::in_place_type<S3PushStep>, s3Bucket, s3Prefix, s3File, localPath)
{
}

PipelineStep::PipelineStep(S3PullStep::Tag,
                           std::string_view s3Bucket,
                           std::string_view s3Prefix,
                           std::string_view s3File,
                           const std::string& localPath)
    :_opcode(PipelineOpcode::S3PULL),
    _impl(std::in_place_type<S3PullStep>, s3Bucket, s3Prefix, s3File, localPath)
{
}

PipelineStep::PipelineStep(CreateEdgeStep::Tag,
                           const EntityPattern* src,
                           const EntityPattern* edge,
                           const EntityPattern* tgt)
    : _opcode(PipelineOpcode::CREATE_EDGE),
    _impl(std::in_place_type<CreateEdgeStep>, src, edge, tgt)
{
}

PipelineStep::PipelineStep(CommitStep::Tag)
    : _opcode(PipelineOpcode::COMMIT),
    _impl(std::in_place_type<CommitStep>)
{
}

SCAN_NODE_PROPERTIES_IMPL(SCAN_NODE_PROPERTY_INT64, Int64)
SCAN_NODE_PROPERTIES_IMPL(SCAN_NODE_PROPERTY_UINT64, UInt64)
SCAN_NODE_PROPERTIES_IMPL(SCAN_NODE_PROPERTY_DOUBLE, Double)
SCAN_NODE_PROPERTIES_IMPL(SCAN_NODE_PROPERTY_STRING, String)
SCAN_NODE_PROPERTIES_IMPL(SCAN_NODE_PROPERTY_BOOL, Bool)

SCAN_NODE_PROPERTIES_AND_LABEL_IMPL(SCAN_NODE_PROPERTY_AND_LABEL_INT64, Int64)
SCAN_NODE_PROPERTIES_AND_LABEL_IMPL(SCAN_NODE_PROPERTY_AND_LABEL_UINT64, UInt64)
SCAN_NODE_PROPERTIES_AND_LABEL_IMPL(SCAN_NODE_PROPERTY_AND_LABEL_DOUBLE, Double)
SCAN_NODE_PROPERTIES_AND_LABEL_IMPL(SCAN_NODE_PROPERTY_AND_LABEL_STRING, String)
SCAN_NODE_PROPERTIES_AND_LABEL_IMPL(SCAN_NODE_PROPERTY_AND_LABEL_BOOL, Bool)

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

GET_FILTERED_PROPERTY_STEP_IMPL(Node, GET_FILTERED_NODE_PROPERTY_INT64, Int64)
GET_FILTERED_PROPERTY_STEP_IMPL(Node, GET_FILTERED_NODE_PROPERTY_UINT64, UInt64)
GET_FILTERED_PROPERTY_STEP_IMPL(Node, GET_FILTERED_NODE_PROPERTY_DOUBLE, Double)
GET_FILTERED_PROPERTY_STEP_IMPL(Node, GET_FILTERED_NODE_PROPERTY_STRING, String)
GET_FILTERED_PROPERTY_STEP_IMPL(Node, GET_FILTERED_NODE_PROPERTY_BOOL, Bool)

GET_FILTERED_PROPERTY_STEP_IMPL(Edge, GET_FILTERED_EDGE_PROPERTY_INT64, Int64)
GET_FILTERED_PROPERTY_STEP_IMPL(Edge, GET_FILTERED_EDGE_PROPERTY_UINT64, UInt64)
GET_FILTERED_PROPERTY_STEP_IMPL(Edge, GET_FILTERED_EDGE_PROPERTY_DOUBLE, Double)
GET_FILTERED_PROPERTY_STEP_IMPL(Edge, GET_FILTERED_EDGE_PROPERTY_STRING, String)
GET_FILTERED_PROPERTY_STEP_IMPL(Edge, GET_FILTERED_EDGE_PROPERTY_BOOL, Bool)

PipelineStep::~PipelineStep() {
}

void PipelineStep::describe(std::string& descr) const {
    std::visit([&](auto& step) { step.describe(descr); }, _impl);
}
