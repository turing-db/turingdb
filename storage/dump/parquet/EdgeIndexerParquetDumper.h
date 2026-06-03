#pragma once

#include "Path.h"

namespace db {

class EdgeIndexer;

// Serializes an EdgeIndexer to three Parquet files: the per-node out/in ranges
// (nodedata) and the out/in label-set span tables. firstNodeID/firstEdgeID and the
// core/patch node counts ride in the nodedata file's key/value metadata.
// _patchNodeOffsets is not written — the loader rebuilds it by scanning each patch
// node's first edge, as the binary EdgeIndexerLoader does. Throws on failure.
class EdgeIndexerParquetDumper {
public:
    static void dump(const EdgeIndexer& indexer,
                     const fs::Path& nodeDataPath,
                     const fs::Path& outSpansPath,
                     const fs::Path& inSpansPath);
};

}
