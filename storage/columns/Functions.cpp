#include "Functions.h"

#include <math.h>

#include <range/v3/view/drop.hpp>

#include "metadata/LabelMap.h"
#include "reader/GraphReader.h"
#include "views/GraphView.h"

#include "BioAssert.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

void LabelsFunction::getLabelString(std::string& out, GraphView view, NodeID n) {
    out.clear();
    const LabelSetHandle lblset = view.read().getNodeLabelSet(n);

    std::vector<LabelID> labels;
    lblset.decompose(labels);

    bioassert(!labels.empty(), "Could not retrieve labels for node {}.", n.getValue());

    const LabelMap& lblMap = view.metadata().labels();

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

    double dot = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (size_t i = 0; i < a.size(); i++) {
        dot += a[i] * b[i];
        normA += a[i] * a[i];
        normB += b[i] * b[i];
    }

    const double denom = sqrt(normA) * sqrt(normB);
    if (denom == 0.0) {
        return 0.0;
    }

    return dot / denom;
}

EuclideanDistanceFunction::ResultType EuclideanDistanceFunction::operator()(const types::Embedding::Primitive& a,
                                                                            const types::Embedding::Primitive& b) {
    bioassert(a.size() == b.size(), "Embedding dimension mismatch in euclidean_distance.");

    double sum = 0.0;
    for (size_t i = 0; i < a.size(); i++) {
        const double diff = a[i] - b[i];
        sum += diff * diff;
    }

    return sqrt(sum);
}
