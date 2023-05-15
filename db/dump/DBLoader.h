#pragma once

#include <filesystem>
#include <string>

namespace db {
class DB;

class DBLoader {
public:
    using Path = std::filesystem::path;

    DBLoader(DB* db, const Path& outDir);
    ~DBLoader();

    void setDBDirectoryName(const std::string& dirName);
    const std::string& getDirName() const { return _dbDirName; }

    bool load();

    static std::string getDefaultDBDirectoryName();

private:
    const Path _outDir;
    std::string _dbDirName;
    DB* _db{nullptr};
};

}
