#include <string>
#include <vector>

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

#include "BioLog.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgImport.h"

#define BIOIMPORT_TOOL_NAME "bioimport"

using namespace Log;
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

int main(int argc, const char** argv) {
    ToolInit toolInit(BIOIMPORT_TOOL_NAME);

    ArgParser& argParser = toolInit.getArgParser();

    argParser.addOption(
        "db-path",
        "Exports the turing database to the specified folder",
        "db_path");

    argParser.addOption(
        "neo4j",
        "Imports a .dump file (default network name: \"my_file\")",
        "my_file.dump");

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

    argParser.addOption(
        "json-neo4j",
        "Imports Neo4j v4 json files from a json/ directory (default network name: \"my_json_dir\")",
        "my_json_dir");

    argParser.addOption(
        "gml",
        "Imports a .gml file (default network name: \"my_file\")",
        "my_file.gml");

    argParser.addOption(
        "csv",
        "Imports a .csv file (default network name: \"my_file\")",
        "my_file.csv");

    argParser.addOption(
        "primary-key",
        "Sets the primary column of a .csv. Must follow a '-csv' option",
        "ColumnName");

    argParser.addOption(
        "net",
        "Sets the name of network. Must follow an import option (-neo4, -gml, ...) ",
        "network_name");

    argParser.addOption(
        "db",
        "Appends the imported data to the existing database "
        "rather than create a new one",
        "turing_db_path");

    toolInit.init(argc, argv);

    std::vector<ImportData> importData;
    std::string turingdbPath;
    std::string existingDbPath;
    db::DB* db = db::DB::create();

    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "db-path") {
            turingdbPath = option.second;
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
        } else if (optName == "json-neo4j") {
            if (!FileUtils::exists(option.second)) {
                BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS()
                            << option.second);
                return EXIT_FAILURE;
            }
            importData.emplace_back(ImportData {
                .type = ImportType::JSON_NEO4J,
                .path = option.second,
            });
        } else if (optName == "gml") {
            if (!FileUtils::exists(option.second)) {
                BioLog::log(msg::ERROR_FILE_NOT_EXISTS()
                            << option.second);
                return EXIT_FAILURE;
            }
            importData.emplace_back(ImportData {
                .type = ImportType::GML,
                .path = option.second,
            });
        } else if (optName == "csv") {
            if (!FileUtils::exists(option.second)) {
                BioLog::log(msg::ERROR_FILE_NOT_EXISTS()
                            << option.second);
                return EXIT_FAILURE;
            }
            importData.emplace_back(ImportData {
                .type = ImportType::CSV,
                .path = option.second,
            });
        } else if (optName == "primary-key") {
            if (importData.empty()) {
                BioLog::log(msg::ERROR_IMPORT_PRIMARY_KEY_APPLIED_WITH_WRONG_ORDER());
                argParser.printHelp();
                return EXIT_FAILURE;
            }

            ImportData& previousCmd = *(importData.end() - 1);

            if (!previousCmd.primaryKey.empty()) {
                BioLog::log(msg::ERROR_IMPORT_PRIMARY_KEY_APPLIED_WITH_WRONG_ORDER());
                argParser.printHelp();
                return EXIT_FAILURE;
            }

            previousCmd.primaryKey = option.second;
        } else if (optName == "net") {
            if (importData.empty()) {
                BioLog::log(msg::ERROR_IMPORT_NET_APPLIED_WITH_WRONG_ORDER());
                argParser.printHelp();
                return EXIT_FAILURE;
            }

            ImportData& previousCmd = *(importData.end() - 1);

            if (!previousCmd.networkName.empty()) {
                BioLog::log(msg::ERROR_IMPORT_NET_APPLIED_WITH_WRONG_ORDER());
                argParser.printHelp();
                return EXIT_FAILURE;
            }

            previousCmd.networkName = option.second;
        } else if (optName == "db") {
            if (!FileUtils::exists(option.second)) {
                BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS()
                            << option.second);
                return EXIT_FAILURE;
            }
            existingDbPath = option.second;
        }
    }

    const bool noPathsGiven = importData.empty();

    if (noPathsGiven) {
        BioLog::log(msg::ERROR_IMPORT_NO_PATH_GIVEN());
        argParser.printHelp();
        return EXIT_SUCCESS;
    }

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
            BioLog::log(msg::WARNING_COULD_NOT_DEDUCE_NETWORK_NAME() << data.path);
            networkName = data.path;
        }

        if (db->getNetwork(db->getString(networkName))) {
            Log::BioLog::log(msg::ERROR_NETWORK_ALREADY_EXISTS() << networkName);
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
                    BioLog::log(msg::ERROR_FAILED_TO_OPEN_FOR_READ() << path.string());
                    return EXIT_FAILURE;
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
                    BioLog::log(msg::ERROR_FAILED_TO_OPEN_FOR_READ() << path.string());
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
            turingdbPath = toolInit.getOutputsDir() / "turing.db";
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
