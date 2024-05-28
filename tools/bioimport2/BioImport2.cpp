#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "BannerDisplay.h"
#include "DB.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "LogUtils.h"
#include "Neo4j/ParserConfig.h"
#include "Neo4jImporter.h"
#include "Time.h"
#include "ToolInit.h"

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

    argParser.add_argument("-j")
        .help("Number of threads to use")
        .metavar("n")
        .default_value(uint16_t(0))
        .store_into(nThreads);

    argParser.add_argument("-neo4j")
        .help("Imports a .dump file (default network name: \"my_file\")")
        .append()
        .metavar("db.dump")
        .action([&](const std::string& value) {
            if (!FileUtils::exists(value)) {
                logt::DirectoryDoesNotExist(value);
                exit(EXIT_FAILURE);
            }
            importData.emplace_back(ImportData {
                .type = ImportType::NEO4J,
                .path = value,
            });
        });

    argParser.add_argument("-neo4j-json")
        .help("Imports json files from a json/ directory (default network name: \"my_json_dir\")")
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

    toolInit.init(argc, argv);

    const bool noPathsGiven = importData.empty();
    if (noPathsGiven) {
        spdlog::error("Please give an import option.");
        toolInit.printHelp();
        return EXIT_FAILURE;
    }

    auto db = std::make_unique<DB>();
    JobSystem jobSystem(nThreads);
    auto t0 = Clock::now();

    jobSystem.initialize();

    for (auto& data : importData) {
        switch (data.type) {
            case ImportType::NEO4J: {
                Neo4jImporter::ImportDumpFileArgs args;
                args._workDir = toolInit.getOutputsDir();
                args._writeFiles = true;
                args._dumpFilePath = std::move(data.path);

                if (!Neo4jImporter::importDumpFile(jobSystem,
                                                   db.get(),
                                                   nodeCountLimit,
                                                   edgeCountLimit,
                                                   args)) {
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
                                              nodeCountLimit,
                                              edgeCountLimit,
                                              args)) {
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
                                                  nodeCountLimit,
                                                  edgeCountLimit,
                                                  args)) {
                    return 1;
                }
                break;
            }
        }
    }

    jobSystem.terminate();
    float dur = duration<Seconds>(t0, Clock::now());
    logt::ElapsedTime(dur, "s");

    return EXIT_SUCCESS;
}
