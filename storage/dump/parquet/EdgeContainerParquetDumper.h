#pragma once

#include "Path.h"

namespace db {

class EdgeContainer;

// Serializes an EdgeContainer to two Parquet files, one per direction: edges-out
// and edges-in, each with four columns (edge_id, node_id, other_id, edge_type_id).
// The first edge id and first node id are stored in each file's key/value metadata.
// Both directions are written in full and read straight back; the in direction is
// not re-derived from the out direction. Throws on failure.
class EdgeContainerParquetDumper {
public:
    static void dump(const EdgeContainer& edges,
                     const fs::Path& outPath,
                     const fs::Path& inPath);
};

}
