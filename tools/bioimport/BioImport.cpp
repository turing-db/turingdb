#include <regex>
#include <vector>
#include <string>

#include "Neo4jImport.h"
#include "JsonParser.h"
#include "JsonParsingStats.h"
#include "Neo4JInstance.h"
#include "ThreadHandler.h"

#include "DB.h"
#include "DBDumper.h"
#include "SchemaReport.h"

#include "PerfStat.h"
#include "TimerStat.h"
#include "ToolInit.h"

#include "FileUtils.h"
#include "BioAssert.h"
#include "BioLog.h"
#include "MsgCommon.h"
#include "MsgImport.h"

#define BIOIMPORT_TOOL_NAME "bioimport"

using namespace Log;
using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit(BIOIMPORT_TOOL_NAME);

    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("neo4j", "imports a neo4j.dump file", true);
    argParser.addOption("jsonNeo4j",
                        "imports json files from a json/ directory (built "
                        "during a previous neo4j.dump import)",
                        true);

    toolInit.init(argc, argv);

    db::DB* db = db::DB::create();

    std::vector<std::string> neo4jDumpPaths;
    std::vector<std::string> jsonDumpPaths;
    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "neo4j") {
            neo4jDumpPaths.emplace_back(option.second);
        } else if (optName == "jsonNeo4j") {
            jsonDumpPaths.emplace_back(option.second);
        }
    }

    if (neo4jDumpPaths.empty() && jsonDumpPaths.empty()) {
        BioLog::log(msg::ERROR_IMPORT_NO_PATH_GIVEN());
        BioLog::printSummary();
        BioLog::destroy();
        PerfStat::destroy();
        return EXIT_SUCCESS;
    }

    {
        Neo4jImport neo4jImport(db, toolInit.getOutputsDir());

        for (const auto& path : neo4jDumpPaths) {
            neo4jImport.importNeo4j(path);
        }

        for (const auto& path : jsonDumpPaths) {
            neo4jImport.importJsonNeo4j(path);
        }
    }

    {
        db::DBDumper dbDumper(db, toolInit.getOutputsDir() / "turing.db");
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
