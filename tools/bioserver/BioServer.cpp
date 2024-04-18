#include "ToolInit.h"

#include "DBServer.h"
#include "DBServerConfig.h"

#include "BioLog.h"

using namespace Log;

int main(int argc, const char** argv) {
    ToolInit toolInit("bioserver");

    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("load", "Loads the requested db at start", "db_name");

    toolInit.init(argc, argv);

    std::vector<std::string> dbNames;

    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "load") {
            dbNames.push_back(option.second);
        }
    }

    // Configuration of the DB Server
    DBServerConfig dbServerConfig;

    // Database server
    DBServer server(dbServerConfig);

    if (!server.run(dbNames)) {
        return EXIT_FAILURE;
    };

    return EXIT_SUCCESS;
}
