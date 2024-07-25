#include "TuringStopCommand.h"

#include <sys/types.h>
#include <signal.h>
#include <spdlog/spdlog.h>

#include "ToolInit.h"
#include "ProcessUtils.h"

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
    ProcessUtils::stopTool("turing-app");
    ProcessUtils::stopTool("turingdb");
    ProcessUtils::stopTool("bioserver");
}
