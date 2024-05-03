#include "TuringStartCommand.h"

#include <spdlog/spdlog.h>
#include <boost/process.hpp>

#include "ToolInit.h"
#include "Command.h"

TuringStartCommand::TuringStartCommand(ToolInit& toolInit)
    : ToolCommand(toolInit),
    _startCommand("start")
{
}

TuringStartCommand::~TuringStartCommand() {
}

void TuringStartCommand::setup() {
    auto& argParser = _toolInit.getArgParser();

    _startCommand.add_description("Start Turing platform");

    argParser.add_subparser(_startCommand);
}

bool TuringStartCommand::isActive() {
    const auto& argParser = _toolInit.getArgParser();
    return argParser.is_subcommand_used("start");
}

void TuringStartCommand::run() {
    Command cmd("turing-app");
    cmd.setWorkingDir(_toolInit.getOutputsDir());
    cmd.setGenerateScript(false);
    cmd.setWriteOnStdout(true);

    /*
    auto child = cmd.runAsync();
    if (!child) {
        spdlog::error("Can not start Turing server, please look at logs of the turing-app command.");
        return;
    }

    child->detach();
    */
}
