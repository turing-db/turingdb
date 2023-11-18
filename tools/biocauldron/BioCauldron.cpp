#include <signal.h>
#include <boost/process.hpp>

#include "TuringUIServer.h"

#include "ToolInit.h"

#include "BioLog.h"
#include "MsgUIServer.h"

#define BIOCAULDRON_TOOL_NAME "biocauldron"

using namespace ui;
using namespace Log;

// This is necessary to handle unix signals
std::unique_ptr<TuringUIServer> server;

void signalHandler(int signum) {
    server->terminate();
    BioLog::printSummary();
    BioLog::destroy();
    exit(EXIT_SUCCESS);
}

int main(int argc, const char** argv) {
    ToolInit toolInit(BIOCAULDRON_TOOL_NAME);

    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("dev", "Use a developpment environment instead of production");
    toolInit.init(argc, argv);

    const bool startDevRequested = argParser.isOptionSet("dev");

    // Create server
    server = std::make_unique<TuringUIServer>(toolInit.getOutputsDir());

    // Install signal handler to handle ctrl+C
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

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
    return EXIT_SUCCESS;
}
