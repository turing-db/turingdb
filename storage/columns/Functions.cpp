#include "Functions.h"

#include <math.h>

#include <algorithm>

#include <range/v3/view/drop.hpp>

#include "datapart/EdgeRecord.h"
#include "list/ListBufferTypeTag.h"
#include "metadata/LabelMap.h"
#include "reader/GraphReader.h"
#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

#include "BioAssert.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

namespace {

// The list a tagged cell holds, or nothing when it holds a null. Only these two answer a
// list function: a cell of any other type is a list function applied to something that is
// no list, which no plan can see coming because the cell carries its type per row.
std::optional<ListView> taggedList(const ListElementView cell) {
    const ListBufferTypeTag tag = cell.getTag();

    if (tag == ListBufferTypeTag::ListView) {
        return cell.getAs<ListView>();
    } else if (tag == ListBufferTypeTag::Null) {
        return std::nullopt;
    }

    throw TuringException("head(), last() and tail() read a list, and this row holds a value that is not one");
}

// The number of Unicode characters a UTF-8 string holds. Every byte of a character but
// its first is a continuation byte, which the two top bits mark as 10, so the characters
// are the bytes that are not marked.
types::Int64::Primitive characterCount(const types::String::Primitive string) {
    return std::count_if(string.begin(), string.end(), [](const char byte) {
        return (static_cast<unsigned char>(byte) & 0xC0) != 0x80;
    });
}

// The edge a tagged cell holds, or nothing when it holds a null. Anything else is an edge
// function applied to something that is no edge, which no plan can see coming because the
// cell carries its type per row.
std::optional<EdgeID> taggedEdge(const ListElementView cell) {
    const ListBufferTypeTag tag = cell.getTag();

    if (tag == ListBufferTypeTag::EdgeID) {
        return cell.getAs<EdgeID>();
    } else if (tag == ListBufferTypeTag::Null) {
        return std::nullopt;
    }

    throw TuringException("startNode() and endNode() read an edge, and this row holds a value that is not one");
}

}

// A row holding no cell at all - an index past the end of a stored list, or a list the row
// does not carry - answers what the cell holding a null answers.
TaggedSizeFunction::ResultType TaggedSizeFunction::operator()(const std::optional<ArgType>& cell) const {
    return cell.has_value() ? (*this)(*cell) : std::nullopt;
}

TaggedSizeFunction::ResultType TaggedSizeFunction::operator()(const ArgType cell) const {
    const ListBufferTypeTag tag = cell.getTag();

    if (tag == ListBufferTypeTag::ListView) {
        return static_cast<types::Int64::Primitive>(cell.getAs<ListView>().size());
    } else if (tag == ListBufferTypeTag::String) {
        return characterCount(cell.getAs<types::String::Primitive>());
    } else if (tag == ListBufferTypeTag::Null) {
        return std::nullopt;
    }

    throw TuringException("size() and length() read a list or a string, and this row holds a value that is neither");
}

StringSizeFunction::ResultType StringSizeFunction::operator()(const ArgType string) const {
    return characterCount(string);
}

TaggedListHeadFunction::ResultType TaggedListHeadFunction::operator()(const std::optional<ArgType>& cell) const {
    return cell.has_value() ? (*this)(*cell) : ListElementView::nullElement();
}

TaggedListHeadFunction::ResultType TaggedListHeadFunction::operator()(const ArgType cell) const {
    const std::optional<ListView> list = taggedList(cell);
    if (!list || list->empty()) {
        return ListElementView::nullElement();
    }

    return list->front();
}

TaggedListLastFunction::ResultType TaggedListLastFunction::operator()(const std::optional<ArgType>& cell) const {
    return cell.has_value() ? (*this)(*cell) : ListElementView::nullElement();
}

TaggedListLastFunction::ResultType TaggedListLastFunction::operator()(const ArgType cell) const {
    const std::optional<ListView> list = taggedList(cell);
    if (!list || list->empty()) {
        return ListElementView::nullElement();
    }

    return list->back();
}

TaggedListTailFunction::ResultType TaggedListTailFunction::operator()(const std::optional<ArgType>& cell) const {
    return cell.has_value() ? (*this)(*cell) : std::nullopt;
}

TaggedListTailFunction::ResultType TaggedListTailFunction::operator()(const ArgType cell) const {
    const std::optional<ListView> list = taggedList(cell);
    if (!list) {
        return std::nullopt;
    }

    return list->tail();
}

LabelsFunction::LabelsFunction(GraphView view, QueryListBuffer* listBuffer)
    : _view(view),
    _listBuffer(listBuffer)
{
}

LabelsFunction::LabelsFunction(GraphView view,
                               QueryListBuffer* listBuffer,
                               const CommitWriteBuffer* writeBuffer)
    : _view(view),
    _listBuffer(listBuffer),
    _writeBuffer(writeBuffer)
{
    if (_writeBuffer) {
        _firstPendingNodeID = _view.read().getTotalNodesAllocated();
    }
}

// A node this change wrote is named by the ID it will commit as - one past the last the
// graph holds, plus its offset in the write buffer - so the ID alone says which of the
// two holds its labels.
bool LabelsFunction::isPendingNode(NodeID node) const {
    return _writeBuffer && node.getValue() >= _firstPendingNodeID;
}

LabelSetHandle LabelsFunction::readLabelSet(NodeID node) const {
    if (isPendingNode(node)) {
        return _writeBuffer->getPendingNode(node.getValue() - _firstPendingNodeID).labelsetHandle;
    }

    return _view.read().getNodeLabelSet(node);
}

LabelsFunction::ResultType LabelsFunction::operator()(const NodeID node) {
    bioassert(_listBuffer, "labels() null list buffer.");

    _elements.clear();

    const bool exists = isPendingNode(node) || _view.read().graphHasNode(node);
    if (!exists) {
        return ListView {};
    }

    const LabelSetHandle lblset = readLabelSet(node);

    _labels.clear();
    lblset.decompose(_labels);

    bioassert(!_labels.empty(), "Could not retrieve labels for node {}.",
              node.getValue());

    const LabelMap& lblMap = _view.metadata().labels();

    for (const LabelID label : _labels) {
        const std::optional<std::string_view> name = lblMap.getName(label);
        _elements.emplace_back(name.value_or("?"));
    }

    return _listBuffer->insert(_elements);
}

EdgeTypesFunction::EdgeTypesFunction(GraphView view)
    : _view(view)
{
}

EdgeTypesFunction::EdgeTypesFunction(GraphView view, const CommitWriteBuffer* writeBuffer)
    : _view(view),
    _writeBuffer(writeBuffer)
{
    if (_writeBuffer) {
        _firstPendingEdgeID = _view.read().getTotalEdgesAllocated();
    }
}

EdgeTypeID EdgeTypesFunction::readEdgeType(EdgeID edge) const {
    const bool isPending = _writeBuffer && edge.getValue() >= _firstPendingEdgeID;
    if (isPending) {
        return _writeBuffer->getPendingEdge(edge.getValue() - _firstPendingEdgeID).edgeType;
    }

    return _view.read().getEdgeTypeID(edge);
}

void EdgeTypesFunction::getEdgeTypeString(std::string& out, EdgeID edge) {
    out.clear();
    const EdgeTypeID et = readEdgeType(edge);

    const EdgeTypeMap& etMap = _view.metadata().edgeTypes();
    const std::optional<std::string_view> name = etMap.getName(et);
    bioassert(name, "Could not get name of EdgeTypeID {}.", et.getValue());

    out = *name;
}

EdgeEndsFunction::EdgeEndsFunction(GraphView view)
    : _view(view)
{
}

EdgeEndsFunction::EdgeEndsFunction(GraphView view, const CommitWriteBuffer* writeBuffer)
    : _view(view),
    _writeBuffer(writeBuffer)
{
    if (_writeBuffer) {
        const GraphReader reader = _view.read();

        _firstPendingNodeID = reader.getTotalNodesAllocated();
        _firstPendingEdgeID = reader.getTotalEdgesAllocated();
    }
}

// A node this change wrote is held in the write buffer under its offset there, and will
// commit as the ID one past the last the graph holds plus that offset. An end the change
// did not write is already a committed ID and stands as it is.
void EdgeEndsFunction::readPendingEnds(EdgeID edge, NodeID& start, NodeID& end) const {
    const CommitWriteBuffer::PendingEdge& pending =
        _writeBuffer->getPendingEdge(edge.getValue() - _firstPendingEdgeID);

    const auto resolve = [this](const CommitWriteBuffer::ExistingOrPendingNode& node) -> NodeID {
        const NodeID* committed = std::get_if<NodeID>(&node);
        if (committed) {
            return *committed;
        }

        return NodeID {std::get<CommitWriteBuffer::PendingNodeOffset>(node) + _firstPendingNodeID};
    };

    start = resolve(pending.src);
    end = resolve(pending.tgt);
}

void EdgeEndsFunction::readEnds(EdgeID edge, NodeID& start, NodeID& end) const {
    const bool isPending = _writeBuffer && edge.getValue() >= _firstPendingEdgeID;
    if (isPending) {
        readPendingEnds(edge, start, end);
        return;
    }

    const EdgeRecord* record = _view.read().getEdge(edge);
    if (!record) {
        return;
    }

    start = record->_nodeID;
    end = record->_otherID;
}

NodeID EdgeEndsFunction::getStartNode(EdgeID edge) const {
    NodeID start;
    NodeID end;
    readEnds(edge, start, end);

    return start;
}

NodeID EdgeEndsFunction::getEndNode(EdgeID edge) const {
    NodeID start;
    NodeID end;
    readEnds(edge, start, end);

    return end;
}

TaggedStartNodeFunction::ResultType TaggedStartNodeFunction::operator()(const std::optional<ArgType>& cell) const {
    return cell.has_value() ? (*this)(*cell) : NodeID {};
}

TaggedStartNodeFunction::ResultType TaggedStartNodeFunction::operator()(const ArgType cell) const {
    const std::optional<EdgeID> edge = taggedEdge(cell);
    if (!edge) {
        return NodeID {};
    }

    return getStartNode(*edge);
}

TaggedEndNodeFunction::ResultType TaggedEndNodeFunction::operator()(const std::optional<ArgType>& cell) const {
    return cell.has_value() ? (*this)(*cell) : NodeID {};
}

TaggedEndNodeFunction::ResultType TaggedEndNodeFunction::operator()(const ArgType cell) const {
    const std::optional<EdgeID> edge = taggedEdge(cell);
    if (!edge) {
        return NodeID {};
    }

    return getEndNode(*edge);
}

TaggedIdFunction::ResultType TaggedIdFunction::operator()(const ArgType cell) const {
    const ListBufferTypeTag tag = cell.getTag();

    if (tag == ListBufferTypeTag::NodeID) {
        return static_cast<types::Int64::Primitive>(cell.getAs<NodeID>().getValue());
    } else if (tag == ListBufferTypeTag::EdgeID) {
        return static_cast<types::Int64::Primitive>(cell.getAs<EdgeID>().getValue());
    } else if (tag == ListBufferTypeTag::Null) {
        return std::nullopt;
    }

    throw TuringException("id() reads a node or an edge, and this row holds a value that is neither");
}

void toBoolFunction::strToLower(std::string& lower, std::string_view src) {
    lower.clear();
    lower.reserve();
    for (const auto c : src) {
        lower += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
}

CosineSimilarityFunction::ResultType CosineSimilarityFunction::operator()(const types::Embedding::Primitive& a,
                                                                          const types::Embedding::Primitive& b) {
    bioassert(a.size() == b.size(), "Embedding dimension mismatch in cosine_similarity.");

    float dot = 0.0f;
    float normA = 0.0f;
    float normB = 0.0f;
    for (size_t i = 0; i < a.size(); i++) {
        dot += a[i] * b[i];
        normA += a[i] * a[i];
        normB += b[i] * b[i];
    }

    const float denom = sqrtf(normA) * sqrtf(normB);
    if (denom == 0.0f) {
        return 0.0;
    }

    return dot / denom;
}

EuclideanDistanceFunction::ResultType EuclideanDistanceFunction::operator()(const types::Embedding::Primitive& a,
                                                                            const types::Embedding::Primitive& b) {
    bioassert(a.size() == b.size(), "Embedding dimension mismatch in euclidean_distance.");

    float sum = 0.0f;
    for (size_t i = 0; i < a.size(); i++) {
        const float diff = a[i] - b[i];
        sum += diff * diff;
    }

    return sqrtf(sum);
}
