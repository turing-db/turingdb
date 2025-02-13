#include "TuringStart2Command.h"

#include <signal.h>
#include <spdlog/spdlog.h>

#include "ToolInit.h"
#include "Command.h"
#include "ProcessUtils.h"

inline void signalHandler(int signum) {
    ProcessUtils::stopTool("turingdb");
    ProcessUtils::stopTool("turing-app2");
    exit(EXIT_SUCCESS);
}

inline void noDemonSignalHandler(int signum) {
    ProcessUtils::stopTool("turingdb");
    exit(EXIT_SUCCESS);
}

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
    _startCommand.add_argument("-nodemon")
        .implicit_value(true)
        .default_value(false);
    _startCommand.add_argument("-p")
        .default_value(_turingDBPort)
        .store_into(_turingDBPort)
        .nargs(1)
        .help("REST API listening port");
#ifdef TURING_DEV
    _startCommand.add_argument("-dev")
        .implicit_value(true)
        .default_value(false);
    _startCommand.add_argument("-build")
        .implicit_value(true)
        .default_value(false);
#endif

    argParser.add_subparser(_startCommand);
}

bool TuringStart2Command::isActive() {
    const auto& argParser = _toolInit.getArgParser();
    return argParser.is_subcommand_used("start2");
}

void TuringStart2Command::run() {
    std::vector<pid_t> turingdbProcs;
    std::vector<pid_t> turingapp2Procs;

    if (!ProcessUtils::searchProcess("turingdb", turingdbProcs)) {
        spdlog::error("Can not search system processes");
        return;
    }

    if (!turingdbProcs.empty()) {
        ProcessUtils::stopTool("turingdb");
    }

    if (!ProcessUtils::searchProcess("turing-app2", turingapp2Procs)) {
        spdlog::error("Can not search system processes");
        return;
    }

    if (!turingapp2Procs.empty()) {
        ProcessUtils::stopTool("turing-app2");
    }

    if (noDemonRequested()) {
        signal(SIGINT, noDemonSignalHandler);
        signal(SIGTERM, noDemonSignalHandler);
    } else {
        signal(SIGINT, signalHandler);
        signal(SIGTERM, signalHandler);
    }

    _toolInit.createOutputDir();
    const auto& outDir = _toolInit.getOutputsDirPath();
    const auto turingAppDir = outDir / "turing-app";
    const auto turingdbDir = outDir / "turingdb";
    const std::string turingDBPort = std::to_string(_turingDBPort);

    Command db("turingdb");
    db.addOption("-o", turingdbDir);
    db.addOption("-p", turingDBPort);
    db.setWorkingDir(outDir);
    db.setGenerateScript(true);
    db.setWriteLogFile(false);
    db.setWriteOnStdout(true);
    db.setScriptPath(outDir / "turingdb.sh");
    db.setLogFile(outDir / "turingdb-launch.log");

    spdlog::info("Starting turingdb");
    if (!db.run()) {
        spdlog::error("Failed to start Turing database server");
        return;
    }

    const int dbExitCode = db.getReturnCode();
    if (dbExitCode != 0) {
        spdlog::error("Failed to start turingdb: turingdb command terminated with exit code {}", dbExitCode);
        return;
    }

    Command turingApp("turing-app2");
    turingApp.addOption("-o", turingAppDir);
    if (noDemonRequested()) {
        turingApp.addArg("-nodemon");
    }
    turingApp.addOption("-p", turingDBPort);
#ifdef TURING_DEV
    if (isDevRequested()) {
        turingApp.addArg("-dev");
    }
    if (isBuildRequested()) {
        turingApp.addArg("-build");
    }
#endif

    turingApp.setWorkingDir(outDir);
    turingApp.setGenerateScript(true);
    turingApp.setWriteLogFile(false);
    turingApp.setWriteOnStdout(true);
    turingApp.setScriptPath(outDir / "turing-app.sh");
    turingApp.setLogFile(outDir / "turing-app-launch.log");

    spdlog::info("Starting turing-app");
    if (!turingApp.run()) {
        spdlog::error("Failed to start Turing application server");
        return;
    }

    const int appExitCode = turingApp.getReturnCode();
    if (appExitCode != 0) {
        spdlog::error("Failed to start turing-app: turing-app command terminated with exit code {}", appExitCode);
        return;
    }
}

bool TuringStart2Command::isDevRequested() {
    auto& argParser = _toolInit.getArgParser();
    auto& startCommand = argParser.at<argparse::ArgumentParser>("start2");
    return startCommand.get<bool>("-dev");
}

bool TuringStart2Command::isBuildRequested() {
    auto& argParser = _toolInit.getArgParser();
    auto& startCommand = argParser.at<argparse::ArgumentParser>("start2");
    return startCommand.get<bool>("-build");
}

bool TuringStart2Command::noDemonRequested() {
    auto& argParser = _toolInit.getArgParser();
    auto& startCommand = argParser.at<argparse::ArgumentParser>("start2");
    return startCommand.get<bool>("-nodemon");
}
