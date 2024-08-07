#include <stdlib.h>
#include <string.h>
#include <memory>
#include <signal.h>
#include <unistd.h>

#include <spdlog/spdlog.h>
#include <argparse.hpp>

#include "ToolInit.h"
#include "BannerDisplay.h"
#include "ProcessUtils.h"

#include "TuringAppServer.h"
#include "Demonology.h"

using namespace app;

namespace {

// This is necessary to handle unix signals
std::unique_ptr<TuringAppServer> server;

void signalHandler(int signum) {
    spdlog::info("Server received signal {}, terminating", signum);
    if (server) {
        server->terminate();
        server->waitAll();
    }
    exit(EXIT_SUCCESS);
}

}

int main(int argc, const char** argv) {
    BannerDisplay::printBanner();

    ToolInit toolInit("turing-app");

    bool isPrototypeMode = false;
    bool isDevMode = false;
    bool buildRequested = false;
    bool noDemonMode = false;

    // Arguments definition
    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("-nodemon")
             .store_into(noDemonMode);

    argParser.add_argument("-prototype")
             .store_into(isPrototypeMode);

#ifdef TURING_DEV
    argParser.add_argument("-dev")
             .store_into(isDevMode);
#endif

    argParser.add_argument("-build")
             .store_into(buildRequested);

    toolInit.init(argc, argv);

    // Demonize
    if (!noDemonMode) {
        Demonology::demonize();
    }

    // Setup app server
    server = std::make_unique<TuringAppServer>(toolInit.getOutputsDir());
    server->setPrototypeMode(isPrototypeMode);
    server->setDevMode(isDevMode);
    server->setBuildRequested(buildRequested);

    // Install signal handler to handle ctrl+C
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // Run server
    server->run();

    spdlog::info("Server components started");

    // Wait for termination
    server->waitAll();

    return EXIT_SUCCESS;
}
