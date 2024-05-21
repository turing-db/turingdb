#pragma once

#include "FileUtils.h"

class Neo4jInstance {
public:
    explicit Neo4jInstance(const FileUtils::Path& baseDir);
    ~Neo4jInstance();

    bool setup();
    bool start();
    bool stop();
    void destroy();
    static bool isRunning();
    static void killJava();

    bool importDumpedDB(const FileUtils::Path& dbFilePath) const;

private:
    const FileUtils::Path _neo4jDir;
    const FileUtils::Path _neo4jBinary;
    const FileUtils::Path _neo4jAdminBinary;
};
