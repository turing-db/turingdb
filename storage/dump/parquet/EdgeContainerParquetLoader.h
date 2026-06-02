#pragma once

#include <memory>

#include "Path.h"

namespace db {

class EdgeContainer;

// Rebuilds an EdgeContainer from the two files written by EdgeContainerParquetDumper,
// loading both the out and in directions directly (the in direction is not
// re-derived from the out direction). Throws on failure (missing metadata, I/O).
class EdgeContainerParquetLoader {
public:
    static std::unique_ptr<EdgeContainer> load(const fs::Path& outPath,
                                               const fs::Path& inPath);
};

}
