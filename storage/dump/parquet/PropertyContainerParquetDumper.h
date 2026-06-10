#pragma once

#include "Path.h"

namespace db {

class PropertyContainer;

// Serializes a single property container (the node-props-<ptID> / edge-props-<ptID>
// payload) to one Parquet file: column 0 "entity_id" (UInt64), column 1 "value"
// typed by the container's ValueType. The value type and, for embeddings, the
// dimension are stored in the file's key/value metadata. Throws on failure.
class PropertyContainerParquetDumper {
public:
    static void dump(const PropertyContainer& props, const fs::Path& path);
};

}
