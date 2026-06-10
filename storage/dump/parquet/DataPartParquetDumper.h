#pragma once

#include "Path.h"

namespace db {

class DataPart;

// Serializes a whole DataPart to a directory of Parquet files by delegating to the
// per-structure adapters — nodes, edges (both directions), the edge indexer, both
// property managers and their property indexers and containers, and both string
// indexers — plus a small info file (data_part_id, first_node_id, first_edge_id).
// Every structure is written in full; nothing is reconstructed on load. Throws on
// failure.
class DataPartParquetDumper {
public:
    static void dump(const DataPart& part, const fs::Path& partDir);
};

}
