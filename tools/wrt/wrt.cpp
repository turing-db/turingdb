#include <memory>
#include <signal.h>
#include <unistd.h>

#include <argparse.hpp>

#include "RegressTesting.h"

#include "ToolInit.h"
#include "BannerDisplay.h"

// This is necessary to handle unix signals
std::unique_ptr<RegressTesting> regress;

void signalHandler(int signum) {
    regress->terminate();
    exit(EXIT_SUCCESS);
}

int main(int argc, const char** argv) {
    BannerDisplay::printBanner();

    ToolInit toolInit("wrt");

    bool cleanDir = false;
    bool cleanIfSuccess = true;
    unsigned concurrency = 1;
    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("-clean")
             .help("Clean all test directories")
             .nargs(0)
             .store_into(cleanDir);
    argParser.add_argument("-noclean")
             .help("Do not clean test directories")
             .nargs(0)
             .action([&](const auto&){ cleanIfSuccess = false; });
    argParser.add_argument("-j")
             .help("Number of concurrent jobs")
             .nargs(1)
             .store_into(concurrency);

    toolInit.init(argc, argv);

    regress = std::make_unique<RegressTesting>(toolInit.getReportsDir());

    // Install signal handler to handle ctrl+C
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    regress->setCleanIfSuccess(cleanIfSuccess);
    regress->setConcurrency(concurrency);

    if (cleanDir) {
        regress->clean();
    } else {
        regress->run();
    }

    if (regress->hasFail()) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

