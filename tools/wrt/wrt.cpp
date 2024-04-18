#include <memory>
#include <signal.h>
#include <unistd.h>

#include "RegressTesting.h"

#include "ToolInit.h"
#include "PerfStat.h"
#include "TimerStat.h"
#include "StringToNumber.h"

#include "BioLog.h"
#include "MsgCommon.h"
#include "MsgWRT.h"

using namespace Log;

// This is necessary to handle unix signals
std::unique_ptr<RegressTesting> regress;

void signalHandler(int signum) {
    regress->terminate();
    exit(EXIT_SUCCESS);
}

int main(int argc, const char** argv) {
    ToolInit toolInit("wrt");
    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("clean", "Clean all test directories");
    argParser.addOption("noclean", "Do not clean test directories");

    toolInit.init(argc, argv);

    bool cleanDir = false;
    bool cleanIfSuccess = true;
    bool error = false;
    for (const auto& option : argParser.options()) {
        if (option.first == "clean") {
            cleanDir = true;
        } else if (option.first == "noclean") {
            cleanIfSuccess = false;
        }
    }

    if (error) {
        return EXIT_FAILURE;
    }

    regress = std::make_unique<RegressTesting>(toolInit.getReportsDir());

    // Install signal handler to handle ctrl+C
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    regress->setCleanIfSuccess(cleanIfSuccess);

    if (cleanDir) {
        regress->clean();
    } else {
        regress->run();
    }

    if (regress->hasFail()) {
        return EXIT_FAILURE;
    }

    return EXIT_FAILURE;
}
