#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "MsgCommon.h"
#include "MsgImport.h"
#include "Neo4jImporter.h"
#include "PerfStat.h"
#include "Time.h"
#include "ToolInit.h"
#include <iostream>

using namespace Log;
using namespace db;

enum class ImportType {
    NEO4J,
    NEO4J_URL,
    JSON_NEO4J,
    // GML,
    // CSV,
};

struct ImportData {
    ImportType type;
    std::string path;
    std::string networkName;
    std::string primaryKey;
    std::string url = "localhost";
    std::string urlSuffix = "/db/data/transaction/commit";
    uint64_t port = 7474;
    std::string username = "neo4j";
    std::string password = "turing";
};

static constexpr size_t nodeCountPerQuery = 400000;
static constexpr size_t edgeCountPerQuery = 1000000;

int main(int argc, const char** argv) {
    ToolInit toolInit("bioimport2");

    ArgParser& argParser = toolInit.getArgParser();

    argParser.addOption(
        "j",
        "Number or threads to use",
        "16");

    argParser.addOption(
        "neo4j",
        "Imports a .dump file (default network name: \"my_file\")",
        "my_file.dump");

    argParser.addOption(
        "neo4j-json",
        "Imports json files from a json/ directory (default network name: \"my_json_dir\")",
        "my_json_dir");

    argParser.addOption(
        "neo4j-url",
        "Imports a neo4j database from an existing neo4j instance",
        "localhost");

    argParser.addOption(
        "port",
        "Port for the query. Must follow a neo4j-url option",
        "7474");

    argParser.addOption(
        "user",
        "Username for the query. Must follow a neo4j-url option",
        "neo4j");

    argParser.addOption(
        "password",
        "Password for the query. Must follow a neo4j-url option",
        "turing");

    argParser.addOption(
        "url-suffix",
        "Suffix for the url. Must follow a neo4j-url option",
        "/db/data/transaction/commit");

    toolInit.init(argc, argv);

    auto db = std::make_unique<DB>();
    JobSystem jobSystem;

    auto t0 = Clock::now();

    std::vector<ImportData> importData;
    uint16_t nThreads = 0;

    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "j") {
            try {
                nThreads = std::stoul(option.second);
            } catch (std::invalid_argument const& ex) {
                BioLog::echo("Number of threads must be an unsigned integer. Got "
                             + option.second + " instead");
                return EXIT_FAILURE;
            }
        } else if (optName == "neo4j") {
            if (!FileUtils::exists(option.second)) {
                BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS()
                            << option.second);
                return EXIT_FAILURE;
            }

            importData.emplace_back(ImportData {
                .type = ImportType::NEO4J,
                .path = option.second,
            });
        } else if (optName == "neo4j-json") {
            if (!FileUtils::exists(option.second)) {
                BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS()
                            << option.second);
                return EXIT_FAILURE;
            }
            importData.emplace_back(ImportData {
                .type = ImportType::JSON_NEO4J,
                .path = option.second,
            });
        } else if (optName == "neo4j-url") {
            importData.emplace_back(ImportData {
                .type = ImportType::NEO4J_URL,
                .url = option.second,
            });
        } else if (optName == "port") {
            if (importData.empty()) {
                BioLog::log(msg::ERROR_URL_NOT_PROVIDED());
                argParser.printHelp();
                return EXIT_FAILURE;
            }
            auto& cmd = importData.back();
            cmd.port = std::stoi(option.second);
        } else if (optName == "user") {
            if (importData.empty()) {
                BioLog::log(msg::ERROR_URL_NOT_PROVIDED());
                argParser.printHelp();
                return EXIT_FAILURE;
            }
            auto& cmd = importData.back();
            cmd.username = option.second;
        } else if (optName == "password") {
            if (importData.empty()) {
                BioLog::log(msg::ERROR_URL_NOT_PROVIDED());
                argParser.printHelp();
                return EXIT_FAILURE;
            }
            auto& cmd = importData.back();
            cmd.password = option.second;
        } else if (optName == "url-suffix") {
            if (importData.empty()) {
                BioLog::log(msg::ERROR_URL_NOT_PROVIDED());
                argParser.printHelp();
                return EXIT_FAILURE;
            }
            auto& cmd = importData.back();
            cmd.urlSuffix = option.second;
        }
    }

    if (importData.empty()) {
        BioLog::log(msg::ERROR_IMPORT_NO_PATH_GIVEN());
        argParser.printHelp();
        return EXIT_SUCCESS;
    }

    jobSystem.initialize(nThreads);

    for (auto& data : importData) {
        switch (data.type) {
            case ImportType::NEO4J: {
                Neo4jImporter::ImportDumpFileArgs args;
                args._workDir = toolInit.getOutputsDir();
                args._writeFiles = true;
                args._dumpFilePath = std::move(data.path);

                if (!Neo4jImporter::importDumpFile(jobSystem,
                                                   db.get(),
                                                   nodeCountPerQuery,
                                                   edgeCountPerQuery,
                                                   args)) {
                    BioLog::log(msg::ERROR_NEO4J_IMPORT());
                    return 1;
                }
                break;
            }
            case ImportType::NEO4J_URL: {
                Neo4jImporter::ImportUrlArgs args;
                args._url = std::move(data.url);
                args._urlSuffix = std::move(data.urlSuffix);
                args._username = std::move(data.username);
                args._password = std::move(data.password);
                args._port = data.port;
                args._workDir = toolInit.getOutputsDir();
                args._writeFiles = true;

                if (!Neo4jImporter::importUrl(jobSystem,
                                              db.get(),
                                              nodeCountPerQuery,
                                              edgeCountPerQuery,
                                              args)) {
                    BioLog::log(msg::ERROR_NEO4J_IMPORT());
                    return 1;
                }
                break;
            }
            case ImportType::JSON_NEO4J: {
                Neo4jImporter::ImportJsonDirArgs args;
                args._jsonDir = std::move(data.path);
                args._workDir = toolInit.getOutputsDir();

                if (!Neo4jImporter::importJsonDir(jobSystem,
                                                       db.get(),
                                                       nodeCountPerQuery,
                                                       edgeCountPerQuery,
                                                       args)) {
                    BioLog::log(msg::ERROR_NEO4J_IMPORT());
                    return 1;
                }
                break;
            }
        }
    }

    jobSystem.terminate();
    float dur = duration<Seconds>(t0, Clock::now());
    BioLog::log(msg::INFO_ELAPSED_TIME() << dur << "s");

    return EXIT_SUCCESS;
}
