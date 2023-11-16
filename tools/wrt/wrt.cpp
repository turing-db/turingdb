#include "RegressTesting.h"

#include "ToolInit.h"
#include "PerfStat.h"
#include "TimerStat.h"
#include "StringToNumber.h"

#include "BioLog.h"
#include "MsgCommon.h"

using namespace Log;

int main(int argc, const char** argv) {
    ToolInit toolInit("wrt");
    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("clean", "Clean all test directories");
    argParser.addOption("timeout", "Maximum running time of a test", "seconds");
    argParser.addOption("noclean", "Do not clean test directories");

    toolInit.init(argc, argv);

    bool cleanDir = false;
    size_t timeout = 0;
    bool cleanIfSuccess = true;
    bool error = false;
    for (const auto& option : argParser.options()) {
        if (option.first == "clean") {
            cleanDir = true;
        } else if (option.first == "timeout") {
            timeout = StringToNumber<size_t>(option.second, error);
            if (error) {
                BioLog::log(msg::ERROR_INCORRECT_CMD_USAGE() << "timeout");
            }
        } else if (option.first == "noclean") {
            cleanIfSuccess = false;
        }
    }

    if (error) {
        BioLog::printSummary();
        BioLog::destroy();
        PerfStat::destroy();
        return EXIT_SUCCESS;
    }

    RegressTesting regress(toolInit.getReportsDir());
    regress.setCleanIfSuccess(cleanIfSuccess);

    if (timeout > 0) {
        regress.setTimeout(timeout);
    }

    if (cleanDir) {
        regress.clean();
    } else {
        regress.run();
    }

    BioLog::printSummary();
    BioLog::destroy();
    PerfStat::destroy();
    return EXIT_SUCCESS;
}
