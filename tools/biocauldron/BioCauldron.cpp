#include "ToolInit.h"

#include "BioLog.h"
#include "MsgUIServer.h"
#include "TuringUIServer.h"
#include <unordered_map>

#define BIOCAULDRON_TOOL_NAME "biocauldron"

using namespace ui;
using namespace Log;

int main(int argc, const char** argv) {
    ToolInit toolInit(BIOCAULDRON_TOOL_NAME);

    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("dev",
                        "Use a developpment environment instead of production",
                        false);
    toolInit.init(argc, argv);

    TuringUIServer server {toolInit.getOutputsDir()};

    argParser.isOptionSet("dev")
        ? server.startDev()
        : server.start();

    server.wait();

    std::unordered_map<ui::ServerType, std::string> serverNames = {
        {ServerType::FLASK,     "Flask"    },
        {ServerType::REACT,     "React"    },
        {ServerType::BIOSERVER, "bioserver"},
    };

    for (uint8_t t = 0; t < (uint8_t)ServerType::_SIZE; t++) {
        int code = server.getReturnCode((ServerType)t);
        if (code != 0) {
            BioLog::log(msg::ERROR_DURING_SERVER_EXECUTION() << serverNames.at((ServerType)t));
            std::string output;
            server.getOutput((ServerType)t, output);
            BioLog::echo(output);
            BioLog::printSummary();
            BioLog::destroy();
            return EXIT_FAILURE;
        }
    }

    BioLog::printSummary();
    BioLog::destroy();
    return EXIT_SUCCESS;
}
