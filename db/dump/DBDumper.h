#pragma once

#include <filesystem>
#include <string>

namespace db {
class DB;

class DBDumper {
public:
    using Path = std::filesystem::path;

    DBDumper(const DB* db, const Path& dbPath);
    ~DBDumper();

    bool dump();

private:
    const Path _dbPath;
    const DB* _db{nullptr};
};

}
