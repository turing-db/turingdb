#include "Functions.h"

#include <math.h>

#include <range/v3/view/drop.hpp>

#include "metadata/LabelMap.h"
#include "reader/GraphReader.h"
#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

#include "BioAssert.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

LabelsFunction::LabelsFunction(GraphView view)
    : _view(view)
{
}

LabelsFunction::LabelsFunction(GraphView view, const CommitWriteBuffer* writeBuffer)
    : _view(view),
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

void LabelsFunction::getLabelString(std::string& out, NodeID node) {
    out.clear();

    const bool exists = isPendingNode(node) || _view.read().graphHasNode(node);
    if (!exists) {
        out = "null";
        return;
    }

    const LabelSetHandle lblset = readLabelSet(node);

    std::vector<LabelID> labels;
    lblset.decompose(labels);

    bioassert(!labels.empty(), "Could not retrieve labels for node {}.", node.getValue());

    const LabelMap& lblMap = _view.metadata().labels();

    {
        const LabelID fstLbl = labels.front();
        const std::optional<std::string_view> fstName = lblMap.getName(fstLbl);
        bioassert(fstName, "Could not get name of LabelID {}.", fstLbl.getValue());
        const std::string_view fstNameUnwrapped = *fstName;

        out = std::string {fstNameUnwrapped};
    }

    for (const LabelID label : labels | rv::drop(1)) {
        out += ", ";

        const std::optional<std::string_view> name = lblMap.getName(label);
        bioassert(name, "Could not get name of LabelID {}.", label.getValue());

        out += *name;
    }
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
