#pragma once

#include "Path.h"

namespace db {

class StringPropertyIndexer;

// Serializes a StringPropertyIndexer (per-property-type prefix-tree indexes) to three
// Parquet files: an indexes table (property_type_id, node_count); a children table
// (property_type_id, parent_node_id, child_index, child_node_id), one row per non-null
// child link; and an owners table (property_type_id, node_id, entity_id), one row per
// owner. Node ids are dense (0..node_count-1) by construction, so the per-node array is
// not stored separately; the tree is read back directly, not rebuilt by re-inserting
// strings. Throws on failure.
class StringPropertyIndexerParquetDumper {
public:
    static void dump(const StringPropertyIndexer& indexer,
                     const fs::Path& indexesPath,
                     const fs::Path& childrenPath,
                     const fs::Path& ownersPath);
};

}
