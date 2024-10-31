#include <cstdlib>
#include <memory>
#include <signal.h>
#include <unistd.h>

#include <spdlog/spdlog.h>
#include <argparse.hpp>

#include "ToolInit.h"
#include "BannerDisplay.h"

#include "TuringApp2Server.h"
#include "Demonology.h"

using namespace app;

namespace {

// This is necessary to handle unix signals
std::unique_ptr<TuringApp2Server> server;

inline void signalHandler(int signum) {
    spdlog::info("Server received signal {}, terminating", signum);
    if (server) {
        server->terminate();
        server->wait();
    }
    exit(EXIT_SUCCESS);
}

}

int main(int argc, const char** argv) {
    BannerDisplay::printBanner();

    ToolInit toolInit("turing-app");

    bool isDevMode = false;
    bool buildRequested = false;
    bool noDemonMode = false;

    // Arguments definition
    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("-nodemon")
             .store_into(noDemonMode);

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
    server = std::make_unique<TuringApp2Server>(toolInit.getOutputsDir());
    server->setDevMode(isDevMode);
    server->setBuildRequested(buildRequested);

    // Install signal handler to handle ctrl+C
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // Run server
    if (!server->run()) {
        spdlog::error("Could not start server");
        return EXIT_FAILURE;
    }

    spdlog::info("Server components started");

    // Wait for termination
    server->wait();

    return EXIT_SUCCESS;
}
