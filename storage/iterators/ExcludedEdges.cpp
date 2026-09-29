#include "ExcludedEdges.h"

#include <algorithm>

#include "BioAssert.h"

using namespace db;

bool ExcludedEdges::holds(std::span<const EdgeID> edges, EdgeID edge) {
    return std::find(edges.begin(), edges.end(), edge) != edges.end();
}

size_t ExcludedEdges::pruneRun(std::span<const EdgeID> excluded,
                               std::span<const EdgeRecord> run,
                               size_t begin,
                               bool consecutiveIDs,
                               ColumnVector<size_t>* indices,
                               ColumnEdgeIDs* edgeIDs,
                               ColumnNodeIDs* others,
                               ColumnEdgeTypes* types) {
    const size_t count = run.size();
    if (excluded.empty() || count == 0) {
        return begin + count;
    }

    const uint64_t first = run.front()._edgeID.getValue();

    if (consecutiveIDs) {
        bioassert(run.back()._edgeID.getValue() == first + count - 1, "The run's edge IDs are not consecutive");

        const auto inRun = [first, count](EdgeID edge) {
            return edge.getValue() >= first && edge.getValue() < first + count;
        };
        if (std::none_of(excluded.begin(), excluded.end(), inRun)) {
            return begin + count;
        }
    }

    size_t kept = begin;
    for (size_t offset = 0; offset < count; offset++) {
        const EdgeID edge = consecutiveIDs ? EdgeID(first + offset) : run[offset]._edgeID;
        if (holds(excluded, edge)) {
            continue;
        }

        const size_t from = begin + offset;
        if (kept != from) {
            (*indices)[kept] = (*indices)[from];

            if (edgeIDs) {
                (*edgeIDs)[kept] = (*edgeIDs)[from];
            }
            if (others) {
                (*others)[kept] = (*others)[from];
            }
            if (types) {
                (*types)[kept] = (*types)[from];
            }
        }

        kept++;
    }

    indices->resize(kept);

    if (edgeIDs) {
        edgeIDs->resize(kept);
    }
    if (others) {
        others->resize(kept);
    }
    if (types) {
        types->resize(kept);
    }

    return kept;
}
