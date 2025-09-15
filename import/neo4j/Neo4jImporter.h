#pragma once

#include <stdint.h>

#include "FileUtils.h"
#include "JobSystem.h"

namespace db {

class Graph;

class Neo4jImporter {
public:
    struct UrlToJsonDirArgs {
        std::string _url = "localhost";
        std::string _urlSuffix = "/db/data/transaction/commit";
        std::string _username = "neo4j";
        std::string _password = "turing";
        uint64_t _port = 7474;
        FileUtils::Path _workDir;
    };
    bool fromUrlToJsonDir(JobSystem& jobSystem,
                          Graph* graph,
                          std::size_t nodeCountPerQuery,
                          std::size_t edgeCountPerQuery,
                          const UrlToJsonDirArgs& args);

    struct ImportJsonDirArgs {
        FileUtils::Path _jsonDir;
        FileUtils::Path _workDir;
    };
    bool importJsonDir(JobSystem& jobSystem,
                       Graph* graph,
                       std::size_t nodeCountPerFile,
                       std::size_t edgeCountPerFile,
                       const ImportJsonDirArgs& args);

    struct DumpFileToJsonDirArgs {
        FileUtils::Path _workDir;
        FileUtils::Path _dumpFilePath;
    };
    bool fromDumpFileToJsonDir(JobSystem& jobSystem,
                               Graph* graph,
                               std::size_t nodeCountPerQuery,
                               std::size_t edgeCountPerQuery,
                               const DumpFileToJsonDirArgs& args);

private:
    std::mutex _mutex;
};

}
