#pragma once

#include "DumpResult.h"
#include "Path.h"

namespace db {

class Commit;
class Graph;

/**
 * @brief Public façade for lazy materialization of a previously-skeleton commit's data.
 * Construction and orchestration are delegated to @ref GraphLoader (the builder) and
 * @ref BinaryDiskDecoder (the byte-level decoder).
 */
class CommitLoader {
public:
    [[nodiscard]] static DumpResult<void> loadData(const fs::Path& commitDir,
                                                   const fs::Path& partsDir,
                                                   Graph* graph,
                                                   Commit* commit);
};

}
