#include "ToolInit.h"

#include <signal.h>
#include <boost/process.hpp>

#include "BioLog.h"
#include "MsgUIServer.h"
#include "TuringUIServer.h"

#define BIOCAULDRON_TOOL_NAME "biocauldron"

using namespace ui;
using namespace Log;

// This is necessary to handle unix signals
std::unique_ptr<TuringUIServer> server;

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

    const bool startDevRequested = argParser.isOptionSet("dev");

    // Create server
    server = std::make_unique<TuringUIServer>(toolInit.getOutputsDir());
    
    // Install signal handler to handle ctrl+C
    signal(SIGINT, sigintHandler);

    // Start server
    if (startDevRequested) {
        server->startDev();
    } else {
        server->start();
    }

    // Wait for termination
    const ui::ServerType serverType = server->waitServerDone();
    const int code = server->getReturnCode(serverType);

    std::string output;
    server->getOutput(serverType, output);

    BioLog::log(msg::ERROR_DURING_SERVER_EXECUTION()
                << SERVER_NAMES.at(serverType)
                << code
                << output);

    server->terminate();

    BioLog::printSummary();
    BioLog::destroy();
    exit(EXIT_SUCCESS);
}
