#include <string>
#include <vector>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "CSVImport.h"
#include "GMLImport.h"
#include "Neo4jImport.h"
#include "StringBuffer.h"

#include "DB.h"
#include "DBDumper.h"
#include "DBLoader.h"
#include "SchemaReport.h"
#include "Writeback.h"

#include "PerfStat.h"
#include "ToolInit.h"
#include "FileUtils.h"
#include "LogUtils.h"
#include "BannerDisplay.h"

#define BIOIMPORT_TOOL_NAME "bioimport"

using namespace db;

enum class ImportType {
    NEO4J,
    NEO4J_URL,
    JSON_NEO4J,
    GML,
    CSV,
};

struct ImportData {
    ImportType type;
    std::string path;
    std::string networkName;
    std::string primaryKey;
    std::string url;
    std::string urlSuffix;
    uint64_t port = 0;
    std::string username;
    std::string password;
};

namespace {

void neo4jURLNotProvided() {
    spdlog::error("Neo4J URL not provided");
}

}

int main(int argc, const char** argv) {
    BannerDisplay::printBanner();

    ToolInit toolInit(BIOIMPORT_TOOL_NAME);

    std::string turingdbPath;
    std::string existingDbPath;
    std::vector<ImportData> importData;

    auto& argParser = toolInit.getArgParser();
    argParser.set_usage_max_line_width(80);

    argParser.add_argument("-db-path")
             .help("Exports the turing database to the specified folder")
             .nargs(1)
             .metavar("dir")
             .store_into(turingdbPath);

    argParser.add_argument("-neo4j")
             .help("Imports a .dump file (default network name: \"my_file\")")
             .append()
             .nargs(1)
             .metavar("db.dump")
             .action([&](const std::string& value){
                if (!FileUtils::exists(value)) {
                    logt::DirectoryDoesNotExist(value);
                    exit(EXIT_FAILURE);
                }
            });

    argParser.add_argument("-gml")
             .help("Imports a .gml file (default network name: \"my_file\")")
             .nargs(1)
             .append()
             .metavar("net.gml")
             .action([&](const std::string& value){
                if (!FileUtils::exists(value)) {
                    logt::FileNotFound(value);
                    exit(EXIT_FAILURE);
                }
                importData.emplace_back(ImportData {
                    .type = ImportType::GML,
                    .path = value,
                });
             });

    argParser.add_argument("-csv")
             .help("Imports a .csv file (default network name: \"my_file\")")
             .nargs(1)
             .append()
             .metavar("data.csv")
             .action([&](const std::string& value){
                if (!FileUtils::exists(value)) {
                    logt::FileNotFound(value);
                    exit(EXIT_FAILURE);
                }
                importData.emplace_back(ImportData {
                    .type = ImportType::CSV,
                    .path = value,
                });
             });

    argParser.add_argument("-neo4j-url")
             .help("Imports a neo4j database from an existing neo4j instance")
             .metavar("localhost")
             .action([&](const std::string& value){
                importData.emplace_back(ImportData {
                    .type = ImportType::NEO4J_URL,
                    .url = value,
                });
            });

    argParser.add_argument("-port")
             .help("Port for the query. Must follow a neo4j-url option")
             .metavar("num")
             .action([&](const std::string& value){
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
             .action([&](const std::string& value){
                if (importData.empty()) {
                    exit(EXIT_FAILURE);
                }
                auto& cmd = importData.back();
                cmd.username = value;
             });

    argParser.add_argument("-password")
             .help("Password for the query. Must follow a neo4j-url option")
             .metavar("pass")
             .action([&](const std::string& value){
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
             .action([&](const std::string& value){
                if (importData.empty()) {
                    neo4jURLNotProvided();
                    exit(EXIT_FAILURE);
                }
                auto& cmd = importData.back();
                cmd.urlSuffix = value;
             });

    argParser.add_argument("-json-neo4j")
             .help("Imports json files from a json/ directory (default network name: \"my_json_dir\")")
             .nargs(1)
             .append()
             .metavar("jsondir")
             .action([&](const std::string& value){
                if (!FileUtils::exists(value)) {
                    logt::DirectoryDoesNotExist(value);
                    exit(EXIT_FAILURE);
                }
                importData.emplace_back(ImportData {
                    .type = ImportType::JSON_NEO4J,
                    .path = value,
                });
             });

    argParser.add_argument("-primary-key")
             .help("Sets the primary column of a .csv. Must follow a '-csv' option")
             .nargs(1)
             .append()
             .metavar("col")
             .action([&](const std::string& value){
                if (importData.size() == 0) {
                    spdlog::error("Primary key can only be applied after existing options");
                    exit(EXIT_FAILURE);
                }

                ImportData& previousCmd = *(importData.end() - 1);

                if (!previousCmd.primaryKey.empty()) {
                    spdlog::error("Primary key can only be applied after existing options");
                    exit(EXIT_FAILURE);
                }

                previousCmd.primaryKey = value;
             });

    argParser.add_argument("-net")
             .help("Sets the name of network. Must follow an import option (-neo4, -gml, ...) ")
             .nargs(1)
             .append()
             .metavar("net_name")
             .action([&](const std::string& value){
                if (importData.size() == 0) {
                    spdlog::error("A network can only be specified after existing options");
                    exit(EXIT_FAILURE);
                }

                ImportData& previousCmd = *(importData.end() - 1);

                if (!previousCmd.networkName.empty()) {
                    spdlog::error("A network can only be specified after existing options");
                    exit(EXIT_FAILURE);
                }

                previousCmd.networkName = value;
             });

    argParser.add_argument("-db")
             .help("Appends the imported data to the existing database")
             .nargs(1)
             .metavar("dbname")
             .store_into(existingDbPath);

    toolInit.init(argc, argv);

    const bool noPathsGiven = importData.empty();
    if (noPathsGiven) {
        spdlog::error("Please give an import option.");
        toolInit.printHelp();
        return EXIT_FAILURE;
    }

    db::DB* db = db::DB::create();

    if (!existingDbPath.empty()) {
        const FileUtils::Path path(existingDbPath);
        DBLoader loader(db, path);
        if (!loader.load()) {
            return EXIT_FAILURE;
        }
    }

    for (const auto& data : importData) {
        Neo4jImport neo4jImport(db, toolInit.getOutputsDir());

        std::string networkName =
            data.networkName.empty()
                ? FileUtils::Path(data.path).stem().string()
                : data.networkName;

        if (networkName.empty()) {
            spdlog::warn("Can not deduce network name");
            networkName = data.path;
        }

        if (db->getNetwork(db->getString(networkName))) {
            spdlog::error("A network named {} already exists", networkName);
            return EXIT_FAILURE;
        }

        switch (data.type) {
            case ImportType::NEO4J: {
                neo4jImport.importNeo4j(data.path, networkName);
                break;
            }
            case ImportType::NEO4J_URL: {
                neo4jImport.importNeo4jUrl(data.url, data.port, data.username,
                                           data.password, data.urlSuffix, networkName);
                break;
            }
            case ImportType::JSON_NEO4J: {
                neo4jImport.importJsonNeo4j(data.path, networkName);
                break;
            }
            case ImportType::GML: {
                const FileUtils::Path path(data.path);
                StringBuffer* strBuffer = StringBuffer::readFromFile(path);
                if (!strBuffer) {
                    logt::CanNotRead(path.string());
                    EXIT_FAILURE;
                }

                Writeback wb(db);
                db::Network* net = wb.createNetwork(db->getString(networkName));
                GMLImport gmlImport(strBuffer, db, net);
                gmlImport.run();
                break;
            }
            case ImportType::CSV: {
                const FileUtils::Path path(data.path);
                StringBuffer* strBuffer = StringBuffer::readFromFile(path);
                if (!strBuffer) {
                    logt::CanNotRead(path.string());
                    return EXIT_FAILURE;
                }

                Writeback wb(db);
                db::Network* net = wb.createNetwork(db->getString(networkName));
                CSVImport csvImport({
                    .buffer = strBuffer,
                    .db = db,
                    .outNet = net,
                    .delimiter = ',',
                    .primaryColumn = data.primaryKey,
                });
                csvImport.run();
                break;
            }
        }
    }

    {
        if (turingdbPath.empty()) {
            turingdbPath = toolInit.getOutputsDir()+"/turing.db";
        }
        db::DBDumper dbDumper(db, turingdbPath);
        dbDumper.dump();
    }

    {
        db::SchemaReport report(toolInit.getReportsDir(), db);
        report.writeReport();
    }

    return EXIT_SUCCESS;
}
