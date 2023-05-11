#pragma once

#include <filesystem>
#include <string>

namespace db {
class DB;

class DBLoader {
public:
    using Path = std::filesystem::path;

    DBLoader(DB** db, const Path& outDir);
    ~DBLoader();

    void setDBDirectoryName(const std::string& dirName);
    const std::string& getDirName() const { return _dbDirName; }
    const Path& getDBPath() const { return _dbPath; }
    const Path& getStringIndexPath() const { return _stringIndexPath; }

    bool load();

    static std::string getDefaultDBDirectoryName();

private:
    const Path _outDir;
    std::string _dbDirName{};
    Path _dbPath{};
    Path _stringIndexPath{};
    DB** _db{nullptr};
};

} // namespace db
