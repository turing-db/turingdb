#include "TuringStart2Command.h"

#include <spdlog/spdlog.h>

#include "ToolInit.h"
#include "Command.h"
#include "ProcessUtils.h"

TuringStart2Command::TuringStart2Command(ToolInit& toolInit)
    : ToolCommand(toolInit),
    _startCommand("start2")
{
}

TuringStart2Command::~TuringStart2Command() {
}

void TuringStart2Command::setup() {
    auto& argParser = _toolInit.getArgParser();

    _startCommand.add_description("Start Turing platform with TuringDB v2");
    argParser.add_subparser(_startCommand);
}

bool TuringStart2Command::isActive() {
    const auto& argParser = _toolInit.getArgParser();
    return argParser.is_subcommand_used("start2");
}

void TuringStart2Command::run() {
    std::vector<pid_t> turingAppProcs;
    std::vector<pid_t> turingDBProcs;
    if (!ProcessUtils::searchProcess("turing-app", turingAppProcs)) {
        spdlog::error("Can not search system processes");
        return;
    }

    if (!ProcessUtils::searchProcess("turingdb", turingDBProcs)) {
        spdlog::error("Can not search system processes");
        return;
    }

    if (!turingAppProcs.empty()) {
        spdlog::error("The tool turing-app is already running with process {}",
                      turingAppProcs[0]);
        return;
    }

    if (!turingDBProcs.empty()) {
        spdlog::error("The tool turingdb is already running with process {}",
                      turingDBProcs[0]);
        return;
    }

    _toolInit.createOutputDir();
    const auto& outDir = _toolInit.getOutputsDirPath();
    const auto turingAppDir = outDir/"turing-app";
    const auto turingDBDir = outDir/"turingdb";

    Command turingApp("turing-app");
    turingApp.addOption("-o", turingAppDir);
    turingApp.setWorkingDir(outDir);
    turingApp.setGenerateScript(true);
    turingApp.setWriteLogFile(false);
    turingApp.setScriptPath(outDir/"turing-app.sh");
    turingApp.setLogFile(outDir/"turing-app-launch.log");

    if (!turingApp.run()) {
        spdlog::error("Failed to start Turing application server");
        return;
    }
    
    const int appExitCode = turingApp.getReturnCode();
    if (appExitCode != 0) {
        spdlog::error("Failed to start turing-app: turing-app command terminated with exit code {}", appExitCode);
        return;
    }

    spdlog::info("Starting turing-app");

    Command db("turingdb");
    db.addOption("-o", turingDBDir);
    db.setWorkingDir(outDir);
    db.setGenerateScript(true);
    db.setWriteLogFile(false);
    db.setScriptPath(outDir/"turingdb.sh");
    db.setLogFile(outDir/"turingdb-launch.log");

    if (!db.run()) {
        spdlog::error("Failed to start Turing database server");
        return;
    }

    const int dbExitCode = db.getReturnCode();
    if (dbExitCode != 0) {
        spdlog::error("Failed to start turingdb: turingdb command terminated with exit code {}", dbExitCode);
        return;
    }

    spdlog::info("Starting turingdb");
}
