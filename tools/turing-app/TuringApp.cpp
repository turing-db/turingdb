#include <stdlib.h>
#include <string.h>
#include <memory>
#include <signal.h>

#include <spdlog/spdlog.h>
#include <argparse.hpp>

#include "ToolInit.h"
#include "BannerDisplay.h"

#include "TuringAppServer.h"

using namespace app;

// This is necessary to handle unix signals
std::unique_ptr<TuringAppServer> server;

void signalHandler(int signum) {
    if (server) {
        server->terminate();
        server->waitAll();
    }
    exit(EXIT_SUCCESS);
}

int main(int argc, const char** argv) {
    BannerDisplay::printBanner();

    ToolInit toolInit("turing-app");

    bool isPrototypeMode = false;
    bool isDevMode = false;

    // Arguments definition
#ifdef TURING_DEV
    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("--prototype")
             .store_into(isPrototypeMode);

    argParser.add_argument("--dev")
             .store_into(isDevMode);
#endif

    toolInit.init(argc, argv);

    server = std::make_unique<TuringAppServer>(toolInit.getOutputsDir());
    server->setPrototypeMode(isPrototypeMode);
    server->setDevMode(isDevMode);

    // Install signal handler to handle ctrl+C
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    server->run();

    // Wait for termination
    server->waitAll();

    return EXIT_SUCCESS;
}
