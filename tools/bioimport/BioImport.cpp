#include <string>
#include <vector>

#include "GMLImport.h"
#include "Neo4jImport.h"
#include "StringBuffer.h"

#include "DB.h"
#include "DBDumper.h"
#include "DBLoader.h"
#include "SchemaReport.h"

#include "PerfStat.h"
#include "ToolInit.h"

#include "BioLog.h"
#include "FileUtils.h"
#include "MsgImport.h"
#include "Writeback.h"

#define BIOIMPORT_TOOL_NAME "bioimport"

using namespace Log;
using namespace db;

enum class ImportType {
    NEO4J,
    JSON_NEO4J,
    GML,
};

struct ImportData {
    ImportType type;
    std::string path;
    std::string networkName;
};

int cleanUp(int returnCode) {
    BioLog::printSummary();
    BioLog::destroy();
    PerfStat::destroy();
    return returnCode;
}

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
        "json-neo4j",
        "Imports json files from a json/ directory (default network name: \"my_json_dir\")",
        "my_json_dir");

    argParser.addOption(
        "gml",
        "Imports a .gml file (default network name: \"my_gml\")",
        "my_gml.gml");

    argParser.addOption(
        "net",
        "Sets the name of network. must follow an import option (-neo4, -gml, ...) ",
        "network_name");

    argParser.addOption(
        "db",
        "Appends the imported data to the existing database "
        "rather than create a new one",
        "turing_db_path");

    toolInit.init(argc, argv);

    std::vector<ImportData> importData;
    std::string turingdbPath = "";
    std::string existingDbPath = "";
    db::DB* db = db::DB::create();

    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "db-path") {
            turingdbPath = option.second;
        } else if (optName == "neo4j") {
            importData.emplace_back(ImportData {
                .type = ImportType::NEO4J,
                .path = option.second,
            });
        } else if (optName == "json-neo4j") {
            importData.emplace_back(ImportData {
                .type = ImportType::JSON_NEO4J,
                .path = option.second,
            });
        } else if (optName == "gml") {
            importData.emplace_back(ImportData {
                .type = ImportType::GML,
                .path = option.second,
            });
        } else if (optName == "net") {
            if (importData.size() == 0) {
                BioLog::log(msg::ERROR_IMPORT_NET_APPLIED_WITH_WRONG_ORDER());
                argParser.printHelp();
                return cleanUp(EXIT_FAILURE);
            }

            ImportData& previousCmd = *(importData.end() - 1);

            if (!previousCmd.networkName.empty()) {
                BioLog::log(msg::ERROR_IMPORT_NET_APPLIED_WITH_WRONG_ORDER());
                argParser.printHelp();
                return cleanUp(EXIT_FAILURE);
            }

            previousCmd.networkName = option.second;
        } else if (optName == "db") {
            existingDbPath = option.second;
        }
    }

    const bool noPathsGiven = importData.empty();

    if (noPathsGiven) {
        BioLog::log(msg::ERROR_IMPORT_NO_PATH_GIVEN());
        argParser.printHelp();
        return cleanUp(EXIT_SUCCESS);
    }

    if (!existingDbPath.empty()) {
        const FileUtils::Path path(existingDbPath);
        DBLoader loader(db, path);
        if (!loader.load()) {
            return cleanUp(EXIT_FAILURE);
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
            return cleanUp(EXIT_FAILURE);
        }

        switch (data.type) {
            case ImportType::NEO4J: {
                neo4jImport.importNeo4j(data.path, networkName);
                break;
            }
            case ImportType::JSON_NEO4J: {
                neo4jImport.importJsonNeo4j(data.path, networkName);
                break;
            }
            case ImportType::GML: {
                const FileUtils::Path path(data.path);
                StringBuffer* strBuffer = StringBuffer::readFromFile(path);
                Writeback wb(db);
                db::Network* net = wb.createNetwork(db->getString(networkName));
                GMLImport gmlImport(strBuffer, db, net, networkName);
                gmlImport.run();
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

    return cleanUp(EXIT_SUCCESS);
}
