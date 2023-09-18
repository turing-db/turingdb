#include "ToolInit.h"

#include <csignal>

#include "BioLog.h"
#include "MsgUIServer.h"
#include "TuringUIServer.h"

#define BIOCAULDRON_TOOL_NAME "biocauldron"

using namespace ui;
using namespace Log;

static std::unique_ptr<TuringUIServer> server;

void sigintHandler(int signum) {
    if (signum == SIGINT) {
        server->terminate();
        BioLog::printSummary();
        BioLog::destroy();
        exit(EXIT_SUCCESS);
    }
}

int main(int argc, const char** argv) {
    ToolInit toolInit(BIOCAULDRON_TOOL_NAME);

    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("dev",
                        "Use a developpment environment instead of production",
                        false);
    toolInit.init(argc, argv);
    signal(SIGINT, sigintHandler);
    server = std::make_unique<TuringUIServer>(toolInit.getOutputsDir());

    argParser.isOptionSet("dev")
        ? server->startDev()
        : server->start();

    const ui::ServerType serverType = server->waitServerDone();
    int code = server->getReturnCode(serverType);

    std::string output;
    server->getOutput(serverType, output);

    BioLog::log(msg::ERROR_DURING_SERVER_EXECUTION()
                << SERVER_NAMES.at(serverType)
                << code
                << output);

    server->terminate();
    BioLog::printSummary();
    BioLog::destroy();
    exit(EXIT_FAILURE);
}
