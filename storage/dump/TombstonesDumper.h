#pragma once

#include "DumpResult.h"

namespace fs {
class FilePageWriter;
}

namespace db {

class Tombstones;

class TombstonesDumper {
public:
    explicit TombstonesDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const Tombstones& tombstones);
    
private:
    fs::FilePageWriter& _writer;
};
    
}
