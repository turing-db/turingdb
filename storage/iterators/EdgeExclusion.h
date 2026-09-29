#pragma once

#include <span>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "datapart/EdgeRecord.h"
#include "ID.h"

namespace db {

class PathTrie;

// The edges a MATCH clause bound before a hop, which its candidates may not repeat: one set
// per input row, read off edge columns and off the paths of an earlier exploration through
// the query's trie, all row-aligned with the hop's input
class EdgeExclusion {
public:
    EdgeExclusion();
    ~EdgeExclusion();

    void setEdgeColumns(std::span<const ColumnEdgeIDs* const> columns);
    void setPathColumns(std::span<const ColumnVector<PathRef>* const> columns, const PathTrie* trie);

    bool isSet() const { return !_edgeColumns.empty() || !_pathColumns.empty(); }

    // Reads the row's excluded edges; excludes and getEdges answer for that row until the next
    void loadRow(size_t row);
    bool excludes(EdgeID edge) const;
    std::span<const EdgeID> getEdges() const { return _edges; }

    // Drops from the columns a writer filled from @p begin on the candidates of the run the
    // loaded row excludes, and returns the columns' new size; a null column is one the
    // writer does not fill. A node's out-edges carry consecutive IDs within a part, so a
    // run of them is searched by arithmetic and no record is read; any other run is scanned
    size_t pruneRun(std::span<const EdgeRecord> run,
                    size_t begin,
                    bool consecutiveIDs,
                    ColumnVector<size_t>* indices,
                    ColumnEdgeIDs* edgeIDs,
                    ColumnNodeIDs* others,
                    ColumnEdgeTypes* types);

    // One hashed bit per edge: a set's OR of them rules an edge out when its bit is clear
    static uint64_t signatureBit(EdgeID edge);

private:
    std::span<const ColumnEdgeIDs* const> _edgeColumns;
    std::span<const ColumnVector<PathRef>* const> _pathColumns;
    const PathTrie* _trie {nullptr};

    std::vector<EdgeID> _edges;
    uint64_t _signature {0};

    // The positions of the excluded edges within the run being pruned
    std::vector<size_t> _positions;

    void collectConsecutivePositions(std::span<const EdgeRecord> run);
    void collectScannedPositions(std::span<const EdgeRecord> run);
    size_t compactRun(size_t begin,
                      size_t count,
                      ColumnVector<size_t>* indices,
                      ColumnEdgeIDs* edgeIDs,
                      ColumnNodeIDs* others,
                      ColumnEdgeTypes* types) const;
};

}
