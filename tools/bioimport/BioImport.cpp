#include <regex>
#include <string>
#include <vector>

#include "GMLImport.h"
#include "JsonParser.h"
#include "JsonParsingStats.h"
#include "Neo4JInstance.h"
#include "Neo4jImport.h"
#include "StringBuffer.h"
#include "ThreadHandler.h"

#include "DB.h"
#include "DBDumper.h"
#include "SchemaReport.h"

#include "PerfStat.h"
#include "TimerStat.h"
#include "ToolInit.h"

#include "BioAssert.h"
#include "BioLog.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgImport.h"
#include "Writeback.h"

#define BIOIMPORT_TOOL_NAME "bioimport"

using namespace Log;
using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit(BIOIMPORT_TOOL_NAME);

    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("turingdbPath", "exports the turing database to the specified folder", true);
    argParser.addOption("neo4j", "imports a neo4j.dump file", true);
    argParser.addOption("jsonNeo4j",
                        "imports json files from a json/ directory (built "
                        "during a previous neo4j.dump import)",
                        true);
    argParser.addOption("gml", "imports a .gml file", true);

    toolInit.init(argc, argv);

    db::DB* db = db::DB::create();

    std::vector<std::string> neo4jDumpPaths;
    std::vector<std::string> jsonDumpPaths;
    std::vector<std::string> gmlDumpPaths;
    std::string turingdbPath = "";

    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "turingdbPath") {
            turingdbPath = option.second;
        } else if (optName == "neo4j") {
            neo4jDumpPaths.emplace_back(option.second);
        } else if (optName == "jsonNeo4j") {
            jsonDumpPaths.emplace_back(option.second);
        } else if (optName == "gml") {
            gmlDumpPaths.emplace_back(option.second);
        }
    }

    bool isNeo4jImport = !neo4jDumpPaths.empty()
                      || !jsonDumpPaths.empty();

    bool isGmlImport = !gmlDumpPaths.empty();

    bool noPathsGiven = !isNeo4jImport && !isGmlImport;

    if (noPathsGiven) {
        BioLog::log(msg::ERROR_IMPORT_NO_PATH_GIVEN());
        argParser.printHelp();
        BioLog::printSummary();
        BioLog::destroy();
        PerfStat::destroy();
        return EXIT_SUCCESS;
    }

    if (isNeo4jImport) {
        Neo4jImport neo4jImport(db, toolInit.getOutputsDir());

        for (const auto& path : neo4jDumpPaths) {
            neo4jImport.importNeo4j(path);
        }

        for (const auto& path : jsonDumpPaths) {
            neo4jImport.importJsonNeo4j(path);
        }
    } else if (isGmlImport) {
        for (const auto& pathStr : gmlDumpPaths) {
            const FileUtils::Path path(pathStr);
            StringBuffer* strBuffer = StringBuffer::readFromFile(path);
            Writeback wb(db);
            db::Network* net = wb.createNetwork(db->getString(path.stem()));
            GMLImport gmlImport(strBuffer, db, net);
            gmlImport.run();
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

    BioLog::printSummary();
    BioLog::destroy();
    PerfStat::destroy();

    return EXIT_SUCCESS;
}
