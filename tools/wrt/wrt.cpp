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
    BioLog::printSummary();
    BioLog::destroy();
    exit(EXIT_SUCCESS);
}

int cleanup(int returnCode) {
    BioLog::printSummary();
    BioLog::destroy();
    PerfStat::destroy();
    return returnCode;
}

int main(int argc, const char** argv) {
    ToolInit toolInit("wrt");
    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("clean", "Clean all test directories");
    argParser.addOption("noclean", "Do not clean test directories");
    argParser.addOption("j", "Number of parallel jobs", "jobs");

    toolInit.init(argc, argv);

    bool cleanDir = false;
    bool cleanIfSuccess = true;
    bool error = false;
    size_t jobs = 1;
    for (const auto& option : argParser.options()) {
        if (option.first == "clean") {
            cleanDir = true;
        } else if (option.first == "noclean") {
            cleanIfSuccess = false;
        } else if (option.first == "j") {
            jobs = StringToNumber<size_t>(option.second, error);
            if (error || jobs == 0) {
                BioLog::log(msg::ERROR_WRT_BAD_NUMBER_OF_JOBS() << option.second);
            }
        }
    }

    if (error) {
        return cleanup(EXIT_FAILURE);
    }

    regress = std::make_unique<RegressTesting>(toolInit.getReportsDir());

    // Install signal handler to handle ctrl+C
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    regress->setCleanIfSuccess(cleanIfSuccess);
    regress->setConcurrency(jobs);

    if (cleanDir) {
        regress->clean();
    } else {
        regress->run();
    }

    if (regress->hasFail()) {
        return cleanup(EXIT_FAILURE);
    }

    return cleanup(EXIT_SUCCESS);
}
