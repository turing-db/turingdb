#pragma once

#include <memory>

#include "Path.h"

namespace db {

class NodeContainer;
class LabelSetMap;

// Rebuilds a NodeContainer from the two files written by NodeContainerParquetDumper,
// loading the ranges and the records directly (no re-derivation). Each serialized
// labelset id is bound to its LabelSetHandle via the supplied LabelSetMap — the
// handle's pointer into label-set storage cannot be serialized, so it is resolved
// against the already-loaded map. Throws on failure (missing metadata, unknown
// labelset, I/O).
class NodeContainerParquetLoader {
public:
    static std::unique_ptr<NodeContainer> load(const fs::Path& rangesPath,
                                               const fs::Path& recordsPath,
                                               const LabelSetMap& labelsets);
};

}
