#pragma once

#include <memory>

#include "Path.h"

namespace db {

class EdgeIndexer;
class EdgeContainer;
class LabelSetMap;

// Rebuilds an EdgeIndexer from the three files written by EdgeIndexerParquetDumper,
// loading the nodedata and label-set spans directly. Span values are bound to the
// supplied EdgeContainer's edge arrays and label-set keys to the supplied LabelSetMap
// (neither pointer can be serialized). _patchNodeOffsets is rebuilt by scanning each
// patch node's first edge for its node id, as the binary EdgeIndexerLoader does.
// Throws on failure.
class EdgeIndexerParquetLoader {
public:
    static std::unique_ptr<EdgeIndexer> load(const fs::Path& nodeDataPath,
                                             const fs::Path& outSpansPath,
                                             const fs::Path& inSpansPath,
                                             const LabelSetMap& labelsets,
                                             const EdgeContainer& edges);
};

}
