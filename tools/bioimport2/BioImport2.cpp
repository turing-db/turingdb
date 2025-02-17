#include <argparse.hpp>
#include <spdlog/spdlog.h>
#include "BannerDisplay.h"
#include "Graph.h"
#include "GraphDumper.h"
#include "FileUtils.h"
#include "GMLImporter.h"
#include "comparators/GraphComparator.h"
#include "GraphLoader.h"
#include "JobSystem.h"
#include "LogUtils.h"
#include "Neo4j/Neo4JParserConfig.h"
#include "Neo4jImporter.h"
#include "Time.h"
#include "ToolInit.h"

using namespace db;
using namespace js;

enum class ImportType {
    NEO4J,
    NEO4J_TO_JSON,
    NEO4J_URL,
    JSON_NEO4J,
    GML,
    BIN,
    // CSV,
};

struct ImportData {
    ImportType type;
    std::string path;
    std::string primaryKey;
    std::string url = "localhost";
    std::string urlSuffix = "/db/data/transaction/commit";
    uint64_t port = 7474;
    std::string username = "neo4j";
    std::string password = "turing";
};

namespace {

void neo4jURLNotProvided() {
    spdlog::error("Neo4J URL not provided");
}

}

int main(int argc, const char** argv) {
    BannerDisplay::printBanner();

    ToolInit toolInit("bioimport2");

    auto& argParser = toolInit.getArgParser();
    argParser.set_usage_max_line_width(80);

    uint16_t nThreads = 0;
    std::vector<ImportData> importData;

    std::string folderPath;

    bool cmpEnabled = false;

    argParser.add_argument("-j")
        .help("Number of threads to use")
        .metavar("n")
        .default_value(uint16_t(0))
        .store_into(nThreads);

    argParser.add_argument("-gml")
        .help("Imports a .gml file")
        .append()
        .metavar("db.gml")
        .action([&](const std::string& value) {
            if (!FileUtils::exists(value)) {
                logt::FileNotFound(value);
                exit(EXIT_FAILURE);
            }
            importData.emplace_back(ImportData {
                .type = ImportType::GML,
                .path = value,
            });
        });

    argParser.add_argument("-db")
        .help("Imports a turingDB binary")
        .append()
        .metavar("db.gml")
        .action([&](const std::string& value) {
            if (!FileUtils::exists(value)) {
                logt::FileNotFound(value);
                exit(EXIT_FAILURE);
            }
            importData.emplace_back(ImportData {
                .type = ImportType::BIN,
                .path = value,
            });
        });

    argParser.add_argument("-neo4j")
        .help("Imports a .dump file")
        .append()
        .metavar("db.dump")
        .action([&](const std::string& value) {
            if (!FileUtils::exists(value)) {
                logt::FileNotFound(value);
                exit(EXIT_FAILURE);
            }
            importData.emplace_back(ImportData {
                .type = ImportType::NEO4J,
                .path = value,
            });
        });

    argParser.add_argument("-neo4j-json")
        .help("Imports json files from a json/ directory")
        .nargs(1)
        .append()
        .metavar("my_json_dir")
        .action([&](const std::string& value) {
            if (!FileUtils::exists(value)) {
                logt::DirectoryDoesNotExist(value);
                exit(EXIT_FAILURE);
            }
            importData.emplace_back(ImportData {
                .type = ImportType::JSON_NEO4J,
                .path = value,
            });
        });

    argParser.add_argument("-neo4j-url")
        .help("Imports a neo4j database from an existing neo4j instance")
        .metavar("localhost")
        .action([&](const std::string& value) {
            importData.emplace_back(ImportData {
                .type = ImportType::NEO4J_URL,
                .url = value,
            });
        });

    argParser.add_argument("-port")
        .help("Port for the query. Must follow a neo4j-url option")
        .metavar("num")
        .action([&](const std::string& value) {
            if (importData.empty()) {
                neo4jURLNotProvided();
                exit(EXIT_FAILURE);
            }
            auto& cmd = importData.back();
            cmd.port = std::stoi(value);
        });

    argParser.add_argument("-user")
        .help("Username for the query. Must follow a neo4j-url option")
        .metavar("username")
        .action([&](const std::string& value) {
            if (importData.empty()) {
                exit(EXIT_FAILURE);
            }
            auto& cmd = importData.back();
            cmd.username = value;
        });

    argParser.add_argument("-password")
        .help("Password for the query. Must follow a neo4j-url option")
        .metavar("pass")
        .action([&](const std::string& value) {
            if (importData.empty()) {
                neo4jURLNotProvided();
                exit(EXIT_FAILURE);
            }
            auto& cmd = importData.back();
            cmd.password = value;
        });

    argParser.add_argument("-url-suffix")
        .help("Suffix for the url. Must follow a neo4j-url option")
        .metavar("/db/data/transaction/commit")
        .action([&](const std::string& value) {
            if (importData.empty()) {
                neo4jURLNotProvided();
                exit(EXIT_FAILURE);
            }
            auto& cmd = importData.back();
            cmd.urlSuffix = value;
        });

    argParser.add_argument("-neo4j-to-json")
        .help("Converts a neo4j dump into json files")
        .append()
        .metavar("db.dump")
        .action([&](const std::string& value) {
            if (!FileUtils::exists(value)) {
                logt::FileNotFound(value);
                exit(EXIT_FAILURE);
            }
            importData.emplace_back(ImportData {
                .type = ImportType::NEO4J_TO_JSON,
                .path = value,
            });
        });

    argParser.add_argument("-db-path")
        .help("Dump the GraphDB binary to a separate directory")
        .metavar("db.dump")
        .action([&](const std::string& value) {
            if (!FileUtils::exists(value)) {
                logt::FileNotFound(value);
                exit(EXIT_FAILURE);
            }
            folderPath = value + "/bindump";
        });

    argParser.add_argument("-cmp")
        .help("Compare Loaded Graphs")
        .nargs(0)
        .action([&](const auto&) {
            cmpEnabled = true;
        });

    toolInit.init(argc, argv);



    if (folderPath.empty()) {
        folderPath = toolInit.getOutputsDir() + "/bindump";
    }

    if(!cmpEnabled){
        const fs::Path binDumpPath {folderPath};
        if (auto res = binDumpPath.mkdir(); !res) {
            spdlog::error("Failed To create bindump directory err: {}", res.error().fmtMessage());
            return 1;
        }
    }


    const bool noPathsGiven = importData.empty();
    if (noPathsGiven) {
        spdlog::error("Please give an import option.");
        toolInit.printHelp();
        return EXIT_FAILURE;
    }

    std::vector<Graph> graphs(importData.size());
    auto graph = std::make_unique<Graph>();
    JobSystem jobSystem(nThreads);
    auto t0 = Clock::now();

    jobSystem.initialize();

    auto graphIt = graphs.begin();
    auto dataIt = importData.begin();

    spdlog::info("import data size is {}", importData.size());

    for (; dataIt != importData.end(); graphIt++, dataIt++) {

        //Get The path we will dump our turingDB binaries to
        size_t pos = dataIt->path.find_last_of('/');
        std::string filePath;

        if (pos == std::string::npos) {
            filePath = {folderPath + dataIt->path};
        } else {
            filePath = {folderPath + dataIt->path.substr(pos)};
        }

        switch (dataIt->type) {
            case ImportType::BIN: {
                if (auto res = GraphLoader::load(&(*graphIt), fs::Path(dataIt->path)); !res) {
                    spdlog::error("Failed To Load Graph: {}", res.error().fmtMessage());
                    return 1;
                }
                break;
            }
            case ImportType::GML: {
                GMLImporter parser;
                if (!parser.importFile(jobSystem, &(*graphIt), FileUtils::Path(dataIt->path))) {
                    return 1;
                }
                break;
            }
            case ImportType::NEO4J: {
                Neo4jImporter::ImportDumpFileArgs args;
                args._workDir = toolInit.getOutputsDir();
                args._writeFiles = true;
                args._dumpFilePath = std::move(dataIt->path);

                if (!Neo4jImporter::importDumpFile(jobSystem,
                                                   &(*graphIt),
                                                   db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                                   db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                                   args)) {
                    return 1;
                }
                break;
            }
            case ImportType::NEO4J_TO_JSON: {
                Neo4jImporter::ImportDumpFileArgs args;
                args._workDir = toolInit.getOutputsDir();
                args._writeFiles = true;
                args._writeFilesOnly = true;
                args._dumpFilePath = std::move(dataIt->path);

                if (!Neo4jImporter::importDumpFile(jobSystem,
                                                   &(*graphIt),
                                                   db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                                   db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                                   args)) {
                    return 1;
                }
                break;
            }
            case ImportType::NEO4J_URL: {
                Neo4jImporter::ImportUrlArgs args;
                args._url = std::move(dataIt->url);
                args._urlSuffix = std::move(dataIt->urlSuffix);
                args._username = std::move(dataIt->username);
                args._password = std::move(dataIt->password);
                args._port = dataIt->port;
                args._workDir = toolInit.getOutputsDir();
                args._writeFilesOnly = false;
                args._writeFiles = true;

                if (!Neo4jImporter::importUrl(jobSystem,
                                              &(*graphIt),
                                              db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                              db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                              args)) {
                    return 1;
                }
                break;
            }
            case ImportType::JSON_NEO4J: {
                Neo4jImporter::ImportJsonDirArgs args;
                args._jsonDir = std::move(dataIt->path);
                args._workDir = toolInit.getOutputsDir();

                if (!Neo4jImporter::importJsonDir(jobSystem,
                                                  &(*graphIt),
                                                  db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                                  db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                                  args)) {
                    return 1;
                }
                break;
            }
        }

        spdlog::info("filePath is {} and folderPath is {}", filePath, folderPath);
        spdlog::info("filePath is {} and folderPath is {}", filePath, folderPath);
        if(!cmpEnabled){
            const fs::Path path {filePath};
            if (auto res = GraphDumper::dump((*graphIt), path); !res) {
                spdlog::error("Failed To Dump Graph at {} err: {}", filePath, res.error().fmtMessage());
                return 1;
            }
        }

        if (cmpEnabled && graphIt != graphs.begin()) {
            if (!GraphComparator::same(*graphIt, *(graphIt - 1))) {
                spdlog::error("graph loaded from:{} is not the same as the one loaded from: {}\n", dataIt->path, (dataIt - 1)->path);
                return 1;
            }
            spdlog::info("graph loaded from:{} is the same as the one loaded from: {}\n", dataIt->path, (dataIt - 1)->path);
        }
    }

    jobSystem.terminate();
    float dur = duration<Seconds>(t0, Clock::now());
    logt::ElapsedTime(dur, "s");

    return EXIT_SUCCESS;
}
