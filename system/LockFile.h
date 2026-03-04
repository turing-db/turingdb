#pragma once

#include "File.h"
#include "Path.h"
#include "LockFileResult.h"

namespace db {

class LockFile {
public:
    LockFile();
    ~LockFile();

    LockFile(const LockFile&) = delete;
    LockFile(LockFile&&) = delete;
    LockFile& operator=(const LockFile&) = delete;
    LockFile& operator=(LockFile&&) = delete;

    void setPath(const fs::Path& p);

    LockFileResult<void> tryLock();
    LockFileResult<size_t> getOwningProcess() const;
    void unlock();
    bool waitUnlock(size_t milliseconds);

private:
    fs::Path _path;
    fs::File _file;
    bool _locked {false};

    LockFileResult<uint64_t> getPid() const;
    LockFileResult<void> writeMetadata();
};

}
