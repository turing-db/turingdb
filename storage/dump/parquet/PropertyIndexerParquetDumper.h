#pragma once

#include "indexers/PropertyIndexer.h"
#include "Path.h"

namespace db {

// Serializes a PropertyIndexer to one Parquet file: rows of
// (property_type_id, labelset_id, offset, count), one per PropertyRange, grouped by
// property type then label set. Ranges are written and read back directly. A flat
// row-per-range layout cannot represent an empty group (a property type with no
// label sets, or a label set with no ranges) — those do not occur for a built
// index, and the dumper throws if it encounters one rather than dropping it.
// Throws on failure.
class PropertyIndexerParquetDumper {
public:
    static void dump(const PropertyIndexer& indexer, const fs::Path& path);
};

}
