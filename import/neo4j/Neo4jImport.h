#pragma once

#include "FileUtils.h"

class Neo4jImport {
public:
    static bool importNeo4j(const FileUtils::Path& outDir,
                            const FileUtils::Path& filepath);

    static bool importJsonNeo4j(const FileUtils::Path& outDir,
                                const FileUtils::Path& jsonDir);
};
