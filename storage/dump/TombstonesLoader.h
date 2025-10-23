#pragma once

#include "dump/DumpResult.h"

namespace fs {
class FilePageReader;
}

namespace db {

class Tombstones;

class TombstonesLoader {
public:
    explicit TombstonesLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<void> load(Tombstones& tombstones);

private:
    fs::FilePageReader& _reader;
};

}
