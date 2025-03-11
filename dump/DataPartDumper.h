#pragma once

#include "Path.h"
#include "DumpResult.h"

namespace db {

class DataPart;

class DataPartDumper {
public:
    [[nodiscard]] static DumpResult<void> dump(const DataPart& part, const fs::Path& path);
};

}
