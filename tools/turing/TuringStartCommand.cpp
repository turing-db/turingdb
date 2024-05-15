#include "TuringStartCommand.h"

#include <spdlog/spdlog.h>

#include "ToolInit.h"
#include "Command.h"
#include "FileUtils.h"

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
    const auto turingAppDir = _toolInit.getOutputsDirPath()/"turing-app";
    Command turingApp("turing-app");
    turingApp.addOption("-o", turingAppDir.string());
    turingApp.setWorkingDir(_toolInit.getOutputsDir());
    turingApp.setGenerateScript(true);
    turingApp.setWriteLogFile(false);
    turingApp.setScriptPath(_toolInit.getOutputsDirPath()/"turing-app.sh");
    turingApp.setLogFile(_toolInit.getOutputsDirPath()/"turing-app-launch.log");

    if (!turingApp.run()) {
        spdlog::error("Failed to start Turing application server");
        return;
    }
    
    const int appExitCode = turingApp.getReturnCode();
    if (appExitCode != 0) {
        spdlog::error("Failed to start turing-app: turing-app command terminated with exit code {}", appExitCode);
        return;
    }

    const auto turingDBDir = _toolInit.getOutputsDirPath()/"turingdb";
    Command db("turingdb");
    db.addOption("-o", turingDBDir.string());
    db.setWorkingDir(_toolInit.getOutputsDir());
    db.setGenerateScript(true);
    db.setWriteLogFile(false);
    db.setScriptPath(_toolInit.getOutputsDirPath()/"turingdb.sh");
    db.setLogFile(_toolInit.getOutputsDirPath()/"turingdb-launch.log");

    if (!db.run()) {
        spdlog::error("Failed to start Turing database server");
        return;
    }

    const int dbExitCode = db.getReturnCode();
    if (dbExitCode != 0) {
        spdlog::error("Failed to start turingdb: turingdb command terminated with exit code {}", dbExitCode);
        return;
    }
}
