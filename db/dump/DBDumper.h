#pragma once

#include <filesystem>
#include <string>

namespace db {
class DB;

class DBDumper {
public:
    using Path = std::filesystem::path;

    DBDumper(DB** db, const Path& outDir);
    ~DBDumper();

    void setDBDirectoryName(const std::string& dirName);
    const std::string& getDirName() const { return _dbDirName; }
    const Path& getDBPath() const { return _dbPath; }
    const Path& getStringIndexPath() const { return _stringIndexPath; }

    bool dump();

    static std::string getDefaultDBDirectoryName();

private:
    const Path _outDir;
    std::string _dbDirName{};
    Path _dbPath{};
    Path _stringIndexPath{};
    DB** _db{nullptr};
};

} // namespace db
