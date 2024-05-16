#include "TuringStopCommand.h"

#include <sys/types.h>
#include <signal.h>
#include <spdlog/spdlog.h>

#include "TuringToolConfig.h"
#include "ToolInit.h"
#include "StringToNumber.h"
#include "ProcessUtils.h"

namespace {

void stopTool(const FileUtils::Path& pidFile) {
    std::string pidStr;
    if (!FileUtils::readContent(pidFile, pidStr)) {
        spdlog::error("Failed to read process ID file {}", pidFile.string());
        return;
    }

    bool convertError = false;
    const pid_t pid = StringToNumber<pid_t>(pidStr, convertError);
    if (convertError) {
        spdlog::error("Failed to read a valid process ID in {}", pidFile.string());
        return;
    }

    if (!ProcessUtils::isProcessRunning(pid)) {
        spdlog::info("It looks like the process {} is already terminated", pid);
        return;
    }

    const int signum = SIGTERM;
    if (kill(pid, signum) < 0) {
        spdlog::error("Failed to send signal {} to process {}. Check that you have the necessary permissions.",
                      signum, pid);
        return;
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

    const auto turingAppDir = outDir/TuringToolConfig::TURING_APP_DIR_NAME;
    const auto turingAppPidFile = turingAppDir/TuringToolConfig::TURING_PID_FILE;
    const auto turingDBDir = outDir/TuringToolConfig::TURING_DB_DIR_NAME;
    const auto turingDBPidFile = turingDBDir/TuringToolConfig::TURING_PID_FILE;
    stopTool(turingAppPidFile);
    stopTool(turingDBPidFile);
}
