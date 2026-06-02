#pragma once

#include "Path.h"

namespace db {

class GraphMetadata;

// Serializes a GraphMetadata's four schema maps to one Parquet file each in the commit
// directory: labels.parquet (label_id, name), edge-types.parquet (edge_type_id, name),
// property-types.parquet (property_type_id, value_type, name), and labelsets.parquet
// (labelset_id, integer_0..integer_3). Entries are written in id order; the ids are
// reproduced on load by replaying getOrCreate. Throws on failure.
class GraphMetadataParquetDumper {
public:
    static void dump(const GraphMetadata& metadata, const fs::Path& commitDir);
};

}
