#pragma once

#include "Path.h"
#include "DumpResult.h"

namespace db {

class Commit;

class CommitDumper {
public:
    [[nodiscard]] static DumpResult<void> dump(const Commit& commit, const fs::Path& path);
};

}
