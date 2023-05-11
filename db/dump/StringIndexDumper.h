#pragma once

#include <filesystem>

namespace db {

class DB;
class StringIndex;

class StringIndexDumper {
public:
    using Path = std::filesystem::path;
    StringIndexDumper(const Path& indexPath);

    bool dump(const StringIndex& index);

private:
    Path _indexPath;
};

}
