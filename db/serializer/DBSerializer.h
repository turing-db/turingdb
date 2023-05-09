#pragma once

#include "SerializerResult.h"
#include <filesystem>
#include <string>

namespace db {
class DB;

class DBSerializer {
public:
    using Path = std::filesystem::path;

    DBSerializer(DB** db, const Path& outDir);
    ~DBSerializer();

    void setDBDirectoryName(const std::string& dirName);

    Serializer::Result load();
    Serializer::Result dump();

    static std::string getDefaultDBDirectoryName();

private:
    const Path _outDir;
    std::string _dbDirName;
    DB** _db{nullptr};
};

} // namespace db
