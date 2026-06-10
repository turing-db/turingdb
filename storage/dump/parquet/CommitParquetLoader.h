#pragma once

#include <memory>

#include "versioning/CommitHash.h"
#include "Path.h"

namespace db {

class Commit;
class VersionController;

// Rebuilds commits from a Parquet commit directory written by CommitParquetDumper,
// mirroring the binary CommitLoader. load() builds the lightweight commit shell (hash,
// previous-commit link, node/edge/datapart counts read from commit-metadata.parquet);
// loadData() materializes a commit's full data (metadata maps, journal, tombstones, and
// the dataparts of its history) and is run only for the head commit, as the binary path
// does. Friend of Commit / CommitData / CommitHistory to fill their private members.
// Throws on failure.
class CommitParquetLoader {
public:
    static std::unique_ptr<Commit> load(VersionController* controller,
                                        CommitHash hash,
                                        const fs::Path& commitDir,
                                        const Commit* prevCommit);

    static void loadData(const fs::Path& commitDir,
                         const fs::Path& partsDir,
                         VersionController* controller,
                         Commit* commit);
};

}
