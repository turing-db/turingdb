#pragma once

#include <filesystem>
#include <string>

namespace db {

class DBLoader {
public:
    using Path = std::filesystem::path;

    DBLoader(const Path& outDir);
    ~DBLoader();

    void setDBDirectoryName(const std::string& dirName);

    bool load();

private:
    const Path _outDir;
    std::string _dbDirName;
};

}
