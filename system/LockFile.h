#pragma once

#include "Path.h"
#include "LockFileResult.h"

namespace db {

class LockFile {
public:
    explicit LockFile(const fs::Path& p);
    ~LockFile();

    LockFile(const LockFile&) = delete;
    LockFile(LockFile&&) = delete;
    LockFile& operator=(const LockFile&) = delete;
    LockFile& operator=(LockFile&&) = delete;

    LockFileResult<void> tryLock();

private:
    fs::Path _path;
    bool _locked {false};

    LockFileResult<uint64_t> getPid();
    LockFileResult<void> writeMetadata();
};

}
