#pragma once

#include <memory>

#include "dump/DumpResult.h"
#include "versioning/CommitHash.h"

namespace db {
class Commit;
class PropertyManager;
class VersionController;
class Graph;

class CommitLoader {
public:
    [[nodiscard]] static DumpResult<std::unique_ptr<Commit>> load(const fs::Path& commitDir,
                                                                  const fs::Path& partsDir,
                                                                  Graph& graph,
                                                                  CommitHash hash,
                                                                  const Commit* prevCommit);
};
}
