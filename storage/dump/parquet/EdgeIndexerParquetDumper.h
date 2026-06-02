#pragma once

#include "Path.h"

namespace db {

class EdgeIndexer;

// Serializes an EdgeIndexer to four Parquet files: the per-node out/in ranges
// (nodedata), the patch-node offset map, and the out/in label-set span tables.
// firstNodeID/firstEdgeID and the core/patch node counts ride in the nodedata
// file's key/value metadata. _patchNodeOffsets is dumped explicitly — the binary
// format rebuilds it by scanning edges on load, but here nothing is reconstructed.
// Throws on failure.
class EdgeIndexerParquetDumper {
public:
    static void dump(const EdgeIndexer& indexer,
                     const fs::Path& nodeDataPath,
                     const fs::Path& patchPath,
                     const fs::Path& outSpansPath,
                     const fs::Path& inSpansPath);
};

}
