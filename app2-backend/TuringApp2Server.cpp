#include "TuringApp2Server.h"
#include "Command.h"
#include "Process.h"

#include <spdlog/spdlog.h>

using namespace app;

TuringApp2Server::TuringApp2Server(const FileUtils::Path& outDir)
    : _outDir(outDir) {
}

TuringApp2Server::~TuringApp2Server() {
}

bool TuringApp2Server::run() {
    FileUtils::Path frontendPath(TURING_FRONTEND_PATH);
    FileUtils::Path logPath = _outDir / "reports" / "frontend.log";

    Command cmd("bun");
    cmd.addArg(std::string("--cwd=") + frontendPath.string());
    cmd.addArg("run");
    if (_devMode) {
        cmd.addArg("dev");
    } else if (_buildRequested) {
        cmd.addArg("build-and-start");
    } else {
        cmd.addArg("start");
    }
    cmd.setWorkingDir(frontendPath);
    cmd.setLogFile(logPath);
    cmd.setGenerateScript(false);
    cmd.setWriteOnStdout(true);
    _bunProcess = cmd.runAsync();
    return _bunProcess != nullptr;
}

void TuringApp2Server::terminate() {
    if (_bunProcess) {
        _bunProcess->terminate();
    }
}

void TuringApp2Server::wait() {
    _bunProcess->wait();
}
