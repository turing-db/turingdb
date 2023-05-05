#pragma once

#include <filesystem>
#include <string>

namespace db {

class DBDumper {
public:
    using Path = std::filesystem::path;

    DBDumper(const Path& outDir);
    ~DBDumper();

    void setDBDirectoryName(const std::string& dirName);

    bool dump();

    static std::string getDefaultDBDirectoryName();

private:
    const Path _outDir;
    std::string _dbDirName;
};

}
