#include "BioLog.h"
#include "FileUtils.h"
#include "JsonParser.h"
#include "JsonParsingStats.h"
#include "MsgCommon.h"
#include "MsgImport.h"
#include "Neo4JInstance.h"
#include "PerfStat.h"
#include "TimerStat.h"
#include "ToolInit.h"
#include "DBDumper.h"
#include "ThreadHandler.h"
#include "Neo4jImport.h"

#include <regex>

#define BIOIMPORT_TOOL_NAME "bioimport"

using namespace Log;

int main(int argc, const char** argv) {
    ToolInit toolInit(BIOIMPORT_TOOL_NAME);

    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("neo4j", "imports a neo4j.dump file", true);
    argParser.addOption("jsonNeo4j",
                        "imports json files from a json/ directory (built "
                        "during a previous neo4j.dump import)",
                        true);

    toolInit.init(argc, argv);

    std::string neo4jFile;
    std::string jsonNeo4jDir;

    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "neo4j") {
            neo4jFile = option.second;
        }
        if (optName == "jsonNeo4j") {
            jsonNeo4jDir = option.second;
        }
    }

    if (!neo4jFile.empty()) {
        Neo4jImport::importNeo4j(toolInit.getOutputsDir(), neo4jFile);
    }

    if (!jsonNeo4jDir.empty()) {
        Neo4jImport::importJsonNeo4j(toolInit.getOutputsDir(), jsonNeo4jDir);
    }

    BioLog::printSummary();
    BioLog::destroy();
    PerfStat::destroy();

    return EXIT_SUCCESS;
}
