#pragma once

#include <filesystem>

namespace db {

class DB;
class StringIndex;

class StringIndexLoader {
public:
    using Path = std::filesystem::path;
    StringIndexLoader(const Path& dbPath);

    bool load(StringIndex& index);

private:
    Path _indexPath;
};

}
