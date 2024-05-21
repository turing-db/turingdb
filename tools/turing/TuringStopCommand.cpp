#include "TuringStopCommand.h"

#include <sys/types.h>
#include <signal.h>
#include <spdlog/spdlog.h>

#include "ToolInit.h"
#include "ProcessUtils.h"

namespace {

void stopTool(const std::string& toolName) {
    std::vector<pid_t> pids;
    if (!ProcessUtils::searchProcess(toolName, pids)) {
        spdlog::error("Can not search system processes");
        return;
    }

    const int signum = SIGTERM;
    for (pid_t pid : pids) {
        if (kill(pid, signum) < 0) {
            spdlog::error("Failed to send signal {} to process {}. Check that you have the necessary permissions.",
                          signum, pid);
        }
        spdlog::info("Stopping {}", toolName);
    }
}

}

TuringStopCommand::TuringStopCommand(ToolInit& toolInit)
    : ToolCommand(toolInit),
    _stopCommand("stop")
{
}

TuringStopCommand::~TuringStopCommand() {
}

void TuringStopCommand::setup() {
    auto& argParser = _toolInit.getArgParser();

    _stopCommand.add_description("Stop Turing platform");

    argParser.add_subparser(_stopCommand);
}

bool TuringStopCommand::isActive() {
    const auto& argParser = _toolInit.getArgParser();
    return argParser.is_subcommand_used("stop");
}

void TuringStopCommand::run() {
    const auto outDir = _toolInit.getOutputsDirPath();
    if (!FileUtils::exists(outDir)) {
        spdlog::error("The Turing output directory can not be found at {}",
                      outDir.string());
        return;
    }

    stopTool("turing-app");
    stopTool("turingdb");
    stopTool("bioserver");
}
