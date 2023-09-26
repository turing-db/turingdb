#include "TuringClient.h"

#include <iostream>
#include <chrono>
#include <ratio>

#include "ToolInit.h"
#include "PerfStat.h"
#include "TimerStat.h"

#include "linenoise.h"

#include "BioLog.h"
#include "MsgShell.h"

using namespace turing;
using namespace Log;

using Clock = std::chrono::system_clock;

int main(int argc, const char** argv) {
    ToolInit toolInit("bioshell");

    toolInit.init(argc, argv);

    TuringClient turing;

    const char* shellPrompt = "turing> ";
    char* line = NULL;
    std::string lineStr;
    while ((line = linenoise(shellPrompt)) != NULL) {
        lineStr = line;
        if (lineStr.empty()) {
            continue;
        }

        const auto timeExecStart = Clock::now();
        turing.exec(lineStr);
        const auto timeExecEnd = Clock::now();
        const std::chrono::duration<double, std::milli> duration = timeExecEnd - timeExecStart;
        std::cout << "Request done in " << duration.count() << " ms.\n";

        linenoiseHistoryAdd(line);
    }

    BioLog::printSummary();
    BioLog::destroy();
    PerfStat::destroy();
    return EXIT_SUCCESS;
}
