#include "EdgeExclusion.h"

#include <algorithm>

#include "list/PathTrie.h"

#include "BioAssert.h"

using namespace db;

EdgeExclusion::EdgeExclusion() {
}

EdgeExclusion::~EdgeExclusion() {
}

void EdgeExclusion::setEdgeColumns(std::span<const ColumnEdgeIDs* const> columns) {
    _edgeColumns = columns;
}

void EdgeExclusion::setPathColumns(std::span<const ColumnVector<PathRef>* const> columns, const PathTrie* trie) {
    _pathColumns = columns;
    _trie = trie;
}

uint64_t EdgeExclusion::signatureBit(EdgeID edge) {
    return 1ull << ((edge.getValue() * 0x9E3779B97F4A7C15ull) >> 58);
}

void EdgeExclusion::loadRow(size_t row) {
    _edges.clear();
    _signature = 0;

    for (const ColumnEdgeIDs* column : _edgeColumns) {
        _edges.push_back((*column)[row]);
    }

    for (const ColumnVector<PathRef>* column : _pathColumns) {
        PathRef current = (*column)[row];
        for (uint64_t depth = _trie->getDepth(current); depth > 0; depth--) {
            const PathTrieEntry& entry = _trie->get(current);
            _edges.push_back(entry._edge);
            current = entry._parent;
        }
    }

    for (const EdgeID edge : _edges) {
        _signature |= signatureBit(edge);
    }
}

bool EdgeExclusion::excludes(EdgeID edge) const {
    if ((_signature & signatureBit(edge)) == 0) {
        return false;
    }

    return std::find(_edges.begin(), _edges.end(), edge) != _edges.end();
}

size_t EdgeExclusion::pruneRun(std::span<const EdgeRecord> run,
                               size_t begin,
                               bool consecutiveIDs,
                               ColumnVector<size_t>* indices,
                               ColumnEdgeIDs* edgeIDs,
                               ColumnNodeIDs* others,
                               ColumnEdgeTypes* types) {
    if (consecutiveIDs) {
        collectConsecutivePositions(run);
    } else {
        collectScannedPositions(run);
    }

    if (_positions.empty()) {
        return begin + run.size();
    }

    return compactRun(begin, run.size(), indices, edgeIDs, others, types);
}

void EdgeExclusion::collectConsecutivePositions(std::span<const EdgeRecord> run) {
    const uint64_t first = run.front()._edgeID.getValue();
    const size_t count = run.size();
    bioassert(run.back()._edgeID.getValue() == first + count - 1, "The run's edge IDs are not consecutive");

    _positions.clear();
    for (const EdgeID edge : _edges) {
        const uint64_t value = edge.getValue();
        if (value >= first && value < first + count) {
            _positions.push_back(value - first);
        }
    }

    std::sort(_positions.begin(), _positions.end());
    _positions.erase(std::unique(_positions.begin(), _positions.end()), _positions.end());
}

void EdgeExclusion::collectScannedPositions(std::span<const EdgeRecord> run) {
    _positions.clear();
    for (size_t offset = 0; offset < run.size(); offset++) {
        if (excludes(run[offset]._edgeID)) {
            _positions.push_back(offset);
        }
    }
}

size_t EdgeExclusion::compactRun(size_t begin,
                                 size_t count,
                                 ColumnVector<size_t>* indices,
                                 ColumnEdgeIDs* edgeIDs,
                                 ColumnNodeIDs* others,
                                 ColumnEdgeTypes* types) const {
    size_t kept = begin;
    size_t nextExcluded = 0;

    for (size_t offset = 0; offset < count; offset++) {
        if (nextExcluded < _positions.size() && _positions[nextExcluded] == offset) {
            nextExcluded++;
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
