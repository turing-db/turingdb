#include "ExcludedEdges.h"

#include <algorithm>

#include "datapart/EdgeContainer.h"

using namespace db;

bool ExcludedEdges::holds(std::span<const EdgeID> edges, EdgeID edge) {
    return std::find(edges.begin(), edges.end(), edge) != edges.end();
}

bool ExcludedEdges::repeatsEarlier(std::span<const EdgeID> edges, size_t position) {
    const auto earlierEnd = edges.begin() + position;

    return std::find(edges.begin(), earlierEnd, edges[position]) != earlierEnd;
}

size_t ExcludedEdges::countInOutRun(std::span<const EdgeID> excluded, const EdgeContainer& part, std::span<const EdgeRecord> outRun) {
    if (outRun.empty()) {
        return 0;
    }

    const EdgeID firstEdgeID = part.getFirstEdgeID();
    const size_t runFirst = outRun.data() - part.getOuts().data();

    size_t held = 0;
    for (size_t position = 0; position < excluded.size(); position++) {
        const EdgeID edge = excluded[position];
        const size_t offset = (edge - firstEdgeID).getValue();
        const bool repeated = repeatsEarlier(excluded, position);

        if (!repeated && offset >= runFirst && offset < runFirst + outRun.size()) {
            held++;
        }
    }

    return held;
}

size_t ExcludedEdges::countInInRun(std::span<const EdgeID> excluded, const EdgeContainer& part, NodeID node) {
    size_t held = 0;
    for (size_t position = 0; position < excluded.size(); position++) {
        const EdgeID edge = excluded[position];
        const bool repeated = repeatsEarlier(excluded, position);

        const EdgeRecord* record = part.tryGet(edge);
        if (!repeated && record && record->_otherID == node) {
            held++;
        }
    }

    return held;
}

size_t ExcludedEdges::copyRunLeavingOut(std::span<const EdgeID> excluded,
                                        size_t held,
                                        std::span<const EdgeRecord> run,
                                        size_t begin,
                                        const EdgeContainer* outPart,
                                        ColumnVector<size_t>* indices,
                                        ColumnEdgeIDs* edgeIDs,
                                        ColumnNodeIDs* others,
                                        ColumnEdgeTypes* types) {
    const size_t count = run.size();

    if (edgeIDs) {
        edgeIDs->resize(begin + count);
    }
    if (others) {
        others->resize(begin + count);
    }
    if (types) {
        types->resize(begin + count);
    }

    const auto nextHeld = [&](size_t from) {
        if (outPart) {
            const EdgeID first = EdgeID(outPart->getFirstEdgeID().getValue() + (run.data() - outPart->getOuts().data()));
            size_t next = count;
            for (const EdgeID edge : excluded) {
                const size_t position = (edge - first).getValue();
                if (position >= from && position < next) {
                    next = position;
                }
            }

            return next;
        } else {
            for (size_t offset = from; offset < count; offset++) {
                const EdgeID edge = run[offset]._edgeID;
                for (const EdgeID excludedEdge : excluded) {
                    if (excludedEdge == edge) {
                        return offset;
                    }
                }
            }

            return count;
        }
    };

    size_t from = 0;
    size_t written = begin;
    size_t found = 0;

    while (from < count) {
        const size_t hole = found < held ? nextHeld(from) : count;
        const EdgeRecord* segmentBegin = run.data() + from;
        const EdgeRecord* segmentEnd = run.data() + hole;

        if (edgeIDs) {
            std::transform(segmentBegin, segmentEnd, edgeIDs->data() + written, [](const EdgeRecord& record) { return record._edgeID; });
        }
        if (others) {
            std::transform(segmentBegin, segmentEnd, others->data() + written, [](const EdgeRecord& record) { return record._otherID; });
        }
        if (types) {
            std::transform(segmentBegin, segmentEnd, types->data() + written, [](const EdgeRecord& record) { return record._edgeTypeID; });
        }

        written += hole - from;
        from = hole + 1;
        found += hole < count;
    }

    indices->resize(written);

    if (edgeIDs) {
        edgeIDs->resize(written);
    }
    if (others) {
        others->resize(written);
    }
    if (types) {
        types->resize(written);
    }

    return written;
}
