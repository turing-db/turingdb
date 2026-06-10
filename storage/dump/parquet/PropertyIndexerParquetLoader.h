#pragma once

#include "indexers/PropertyIndexer.h"
#include "Path.h"

namespace db {

class LabelSetMap;

// Rebuilds a PropertyIndexer into the output reference from the file written by
// PropertyIndexerParquetDumper. Each label-set id is bound to its handle via the
// supplied LabelSetMap; ranges are appended directly (nothing recomputed). The
// output indexer is expected to be empty. Throws on failure (unknown labelset, I/O).
class PropertyIndexerParquetLoader {
public:
    static void load(const fs::Path& path, const LabelSetMap& labelsets, PropertyIndexer& out);
};

}
