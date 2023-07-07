#pragma once

#include <filesystem>
#include <string>

namespace db {
class DB;

class DBLoader {
public:
    using Path = std::filesystem::path;

    DBLoader(DB* db, const Path& dbDir);
    ~DBLoader();

    bool load();

private:
    const Path _dbDir;
    DB* _db{nullptr};
};

}
