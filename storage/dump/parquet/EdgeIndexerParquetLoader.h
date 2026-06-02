#pragma once

#include <memory>

#include "Path.h"

namespace db {

class EdgeIndexer;
class EdgeContainer;
class LabelSetMap;

// Rebuilds an EdgeIndexer from the four files written by EdgeIndexerParquetDumper,
// loading the nodedata, patch offsets, and label-set spans directly. Span values
// are bound to the supplied EdgeContainer's edge arrays and label-set keys to the
// supplied LabelSetMap (neither pointer can be serialized). _patchNodeOffsets is
// read from the dump, not rebuilt by scanning edges. Throws on failure.
class EdgeIndexerParquetLoader {
public:
    static std::unique_ptr<EdgeIndexer> load(const fs::Path& nodeDataPath,
                                             const fs::Path& patchPath,
                                             const fs::Path& outSpansPath,
                                             const fs::Path& inSpansPath,
                                             const LabelSetMap& labelsets,
                                             const EdgeContainer& edges);
};

}
