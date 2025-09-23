#pragma once

#include "DumpResult.h"
#include "FilePageReader.h"
namespace db {

class LoadUtils {
public:
    [[nodiscard]] static DumpResult<void> loadVector(fs::FilePageReader& reader, size_t sz);
};

}
