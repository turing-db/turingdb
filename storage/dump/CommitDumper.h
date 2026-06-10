#pragma once

#include "Path.h"
#include "DumpResult.h"

namespace db {

class Commit;

class CommitDumper {
public:
    [[nodiscard]] static DumpResult<void> dump(const Commit& commit,
                                               const fs::Path& commitDir,
                                               const fs::Path& partsDir);

private:
    [[nodiscard]] static DumpResult<void> dumpMetaDataFile(const Commit& commit,
                                                           const fs::Path& commitDir);
};

}
