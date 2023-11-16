#pragma once

#include "FileUtils.h"

namespace db {
class DB;
}

class Neo4jImport {
public:
    using Path = FileUtils::Path;

    Neo4jImport(db::DB* db, const Path& outDir);
    ~Neo4jImport();

    bool importNeo4j(const Path& filepath, const std::string& networkName);

    bool importJsonNeo4j(const Path& jsonDir, const std::string& networkName);

private:
    db::DB* _db {nullptr};
    const Path _outDir;
};
