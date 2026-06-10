#pragma once

#include "Path.h"

namespace db {

class Tombstones;

// Serializes a Tombstones' two id sets to two Parquet files in the commit directory:
// tombstone-nodes.parquet (node_id) and tombstone-edges.parquet (edge_id), one row per
// tombstoned id. The sets are unordered, so row order is arbitrary and not significant.
// Throws on failure.
class TombstonesParquetDumper {
public:
    static void dump(const Tombstones& tombstones, const fs::Path& commitDir);
};

}
