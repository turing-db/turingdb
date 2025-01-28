#pragma once

#include <stdint.h>

#include "FileUtils.h"
#include "JobSystem.h"

namespace db {

class Graph;

class Neo4jImporter {
public:
    struct ImportUrlArgs {
        std::string _url = "localhost";
        std::string _urlSuffix = "/db/data/transaction/commit";
        std::string _username = "neo4j";
        std::string _password = "turing";
        uint64_t _port = 7474;
        bool _writeFiles = true;
        bool _writeFilesOnly = false;
        FileUtils::Path _workDir;
    };

    static bool importUrl(JobSystem& jobSystem,
                          Graph* graph,
                          std::size_t nodeCountPerQuery,
                          std::size_t edgeCountPerQuery,
                          const ImportUrlArgs& args);

    struct ImportJsonDirArgs {
        FileUtils::Path _jsonDir;
        FileUtils::Path _workDir;
    };

    static bool importJsonDir(JobSystem& jobSystem,
                              Graph* graph,
                              std::size_t nodeCountPerFile,
                              std::size_t edgeCountPerFile,
                              const ImportJsonDirArgs& args);

    struct ImportDumpFileArgs {
        FileUtils::Path _workDir;
        FileUtils::Path _dumpFilePath;
        bool _writeFiles = true;
        bool _writeFilesOnly = false;
    };

    static bool importDumpFile(JobSystem& jobSystem,
                               Graph* graph,
                               std::size_t nodeCountPerQuery,
                               std::size_t edgeCountPerQuery,
                               const ImportDumpFileArgs& args);
};

}
