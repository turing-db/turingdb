#pragma once

#include "Path.h"

namespace db {

class GraphMetadata;

// Rebuilds a GraphMetadata's four schema maps from the files written by
// GraphMetadataParquetDumper. Each map is repopulated through its public getOrCreate in
// id order, and the reassigned id is asserted to match the dumped id (nothing is
// recomputed). The output GraphMetadata is expected to be empty. Friend of
// GraphMetadata to reach its private maps. Throws on failure (id mismatch, I/O, decode).
class GraphMetadataParquetLoader {
public:
    static void load(const fs::Path& commitDir, GraphMetadata& out);
};

}
