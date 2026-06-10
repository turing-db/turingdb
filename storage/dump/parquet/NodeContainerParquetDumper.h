#pragma once

#include "Path.h"

namespace db {

class NodeContainer;

// Serializes a NodeContainer to two Parquet files: a ranges table
// (labelset_id, first_node_id, count) and a per-node records column (labelset_id).
// The first node id is stored in the records file's key/value metadata. Both tables
// are written in full and read straight back; nothing is re-derived on load.
// Throws on failure.
class NodeContainerParquetDumper {
public:
    static void dump(const NodeContainer& nodes,
                     const fs::Path& rangesPath,
                     const fs::Path& recordsPath);
};

}
