#pragma once

#include "StringRef.h"

#include <filesystem>
#include <vector>

namespace db {

class StringIndex;

class StringIndexLoader {
public:
    using Path = std::filesystem::path;

    StringIndexLoader(const Path& dbPath);

    bool load(StringIndex& index, std::vector<StringRef>& stringRefs);

private:
    Path _indexPath;
};

}
