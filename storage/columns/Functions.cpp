#include "Functions.h"

#include <math.h>

#include <range/v3/view/drop.hpp>

#include "list/ListBufferTypeTag.h"
#include "metadata/LabelMap.h"
#include "reader/GraphReader.h"
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

    throw TuringException("size(), head() and tail() read a list, and this row holds a value that is not one");
}

}

TaggedListSizeFunction::ResultType TaggedListSizeFunction::operator()(const ArgType cell) const {
    const std::optional<ListView> list = taggedList(cell);
    if (!list) {
        return std::nullopt;
    }

    return static_cast<types::Int64::Primitive>(list->size());
}

TaggedListHeadFunction::ResultType TaggedListHeadFunction::operator()(const ArgType cell) const {
    const std::optional<ListView> list = taggedList(cell);
    if (!list || list->empty()) {
        return ListElementView::nullElement();
    }

    return list->front();
}

TaggedListTailFunction::ResultType TaggedListTailFunction::operator()(const ArgType cell) const {
    const std::optional<ListView> list = taggedList(cell);
    if (!list) {
        return std::nullopt;
    }

    return list->tail();
}

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
