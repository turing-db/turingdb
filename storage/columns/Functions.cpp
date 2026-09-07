#include "Functions.h"

#include <math.h>

#include "metadata/LabelMap.h"
#include "reader/GraphReader.h"
#include "views/GraphView.h"

#include "BioAssert.h"

using namespace db;

std::string_view LabelsFunction::getLabelString(NodeID n) {
    const LabelSetHandle lblset = _view.read().getNodeLabelSet(n);

    std::vector<LabelID> labels;
    lblset.decompose(labels);

    bioassert(!labels.empty(), "Could not retrieve labels for node {}.", n.getValue());

    const LabelMap& lblMap = _view.metadata().labels();

    _names.clear();
    _names.reserve(labels.size());

    for (const LabelID label : labels) {
        const std::optional<std::string_view> name = lblMap.getName(label);
        bioassert(name, "Could not get name of LabelID {}.", label.getValue());

        _names.emplace_back(*name);
    }

    return _buffer->join(_names, ", ");
}

void EdgeTypesFunction::getEdgeTypeString(std::string& out, GraphView view, EdgeID e) {
    out.clear();
    const EdgeTypeID et = view.read().getEdgeTypeID(e);

    const EdgeTypeMap& etMap = view.metadata().edgeTypes();
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
