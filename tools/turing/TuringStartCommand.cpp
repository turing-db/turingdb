#include "TuringStartCommand.h"

#include <signal.h>
#include <spdlog/spdlog.h>

#include "ToolInit.h"
#include "Command.h"
#include "ProcessUtils.h"

inline void signalHandler(int signum) {
    ProcessUtils::stopTool("bioserver");
    exit(EXIT_SUCCESS);
}

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
    _startCommand.add_argument("-db")
                 .metavar("dbname")
                 .default_value("reactome")
                 .nargs(1);
    _startCommand.add_argument("-prototype")
                 .implicit_value(true)
                 .default_value(false);
    _startCommand.add_argument("-nodemon")
                 .implicit_value(true)
                 .default_value(false);
#ifdef TURING_DEV
    _startCommand.add_argument("-dev")
                 .implicit_value(true)
                 .default_value(false);
#endif
    _startCommand.add_argument("-build")
                 .implicit_value(true)
                 .default_value(false);

    argParser.add_subparser(_startCommand);
}

bool TuringStartCommand::isActive() {
    const auto& argParser = _toolInit.getArgParser();
    return argParser.is_subcommand_used("start");
}

void TuringStartCommand::run() {
    std::vector<pid_t> turingAppProcs;
    std::vector<pid_t> bioserverProcs;
    if (!ProcessUtils::searchProcess("turing-app", turingAppProcs)) {
        spdlog::error("Can not search system processes");
        return;
    }

    if (!ProcessUtils::searchProcess("bioserver", bioserverProcs)) {
        spdlog::error("Can not search system processes");
        return;
    }

    if (!turingAppProcs.empty()) {
        spdlog::error("The tool turing-app is already running with process {}",
                      turingAppProcs[0]);
        return;
    }

    if (!bioserverProcs.empty()) {
        spdlog::error("The tool bioserver is already running with process {}",
                      bioserverProcs[0]);
        return;
    }

    if (noDemonRequested()) {
        signal(SIGINT, signalHandler);
        signal(SIGTERM, signalHandler);
    }

    _toolInit.createOutputDir();
    const auto dbName = getDBName();
    const auto& outDir = _toolInit.getOutputsDirPath();
    const auto turingAppDir = outDir/"turing-app";
    const auto bioServerDir = outDir/"bioserver";

    Command db("bioserver");
    db.addOption("-o", bioServerDir);
    db.addOption("-db", dbName);
    db.setWorkingDir(outDir);
    db.setGenerateScript(true);
    db.setWriteLogFile(false);
    db.setWriteOnStdout(true);
    db.setScriptPath(outDir/"bioserver.sh");
    db.setLogFile(outDir/"bioserver-launch.log");

    spdlog::info("Starting bioserver");
    if (!db.run()) {
        spdlog::error("Failed to start Turing database server");
        return;
    }

    const int dbExitCode = db.getReturnCode();
    if (dbExitCode != 0) {
        spdlog::error("Failed to start bioserver: bioserver command terminated with exit code {}", dbExitCode);
        return;
    }

    Command turingApp("turing-app");
    turingApp.addOption("-o", turingAppDir);
    if (isPrototypeRequested()) {
        turingApp.addArg("-prototype");
    }
    if (noDemonRequested()) {
        turingApp.addArg("-nodemon");
    }
#ifdef TURING_DEV
    if (isDevRequested()) {
        turingApp.addArg("-dev");
    }
#endif
    if (isBuildRequested()) {
        turingApp.addArg("-build");
    }

    turingApp.setWorkingDir(outDir);
    turingApp.setGenerateScript(true);
    turingApp.setWriteLogFile(false);
    turingApp.setWriteOnStdout(true);
    turingApp.setScriptPath(outDir/"turing-app.sh");
    turingApp.setLogFile(outDir/"turing-app-launch.log");

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

std::string TuringStartCommand::getDBName() {
    auto& argParser = _toolInit.getArgParser();
    auto& startCommand = argParser.at<argparse::ArgumentParser>("start");
    const std::string& dbName = startCommand.get<std::string>("-db");
    return dbName;
}

bool TuringStartCommand::isPrototypeRequested() {
    auto& argParser = _toolInit.getArgParser();
    auto& startCommand = argParser.at<argparse::ArgumentParser>("start");
    return startCommand.get<bool>("-prototype");
}

bool TuringStartCommand::isDevRequested() {
    auto& argParser = _toolInit.getArgParser();
    auto& startCommand = argParser.at<argparse::ArgumentParser>("start");
    return startCommand.get<bool>("-dev");
}

bool TuringStartCommand::isBuildRequested() {
    auto& argParser = _toolInit.getArgParser();
    auto& startCommand = argParser.at<argparse::ArgumentParser>("start");
    return startCommand.get<bool>("-build");
}

bool TuringStartCommand::noDemonRequested() {
    auto& argParser = _toolInit.getArgParser();
    auto& startCommand = argParser.at<argparse::ArgumentParser>("start");
    return startCommand.get<bool>("-nodemon");
}
