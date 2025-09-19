#pragma once

#include "DumpResult.h"
#include "FilePageWriter.h"
namespace db {

class DumpUtils {
public:
    template<typename T>
    DumpResult<void> dumpVector(const std::vector<T> vec, fs::FilePageWriter& wr);
};

}

