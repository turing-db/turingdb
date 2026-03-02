#pragma once

#include "dump/DumpResult.h"
#include "versioning/CommitHash.h"

namespace db {
class Commit;
class Graph;

class CommitLoader {
public:
    [[nodiscard]] static DumpResult<void> loadData(const fs::Path& commitDir,
                                                                  const fs::Path& partsDir,
                                                                  Graph* graph,
                                                                  Commit* commit);
    //Just Load A Commit With No CommitData
    [[nodiscard]] static DumpResult<std::unique_ptr<Commit>> load(Graph* graph,
                                                                  CommitHash hash,
                                                                  const fs::Path& commitDir,
                                                                  const Commit* prevCommit);
};

}
