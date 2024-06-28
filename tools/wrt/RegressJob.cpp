#include "RegressJob.h"

#include <signal.h>
#include <spdlog/spdlog.h>

#include "Command.h"
#include "ProcessUtils.h"
#include "LogUtils.h"
#include "Process.h"

RegressJob::RegressJob(const Path& path)
    : _path(path)
{
}

RegressJob::~RegressJob() {
}

bool RegressJob::start() {
    const auto runScriptPath = _path/"run.sh";
    if (!FileUtils::exists(runScriptPath)) {
        logt::FileNotFound(runScriptPath.string());
        return false;
    }

    Command cmd(runScriptPath.string());
    cmd.setWorkingDir(_path);
    cmd.setLogFile(getLogPath());
    cmd.setGenerateScript(true);

    spdlog::info("Run: {}", _path.string());
    _process = cmd.runAsync();
    return true;
}

bool RegressJob::isRunning() {
    if (!_process) {
        return false;
    }
    return _process->isRunning();
}

int RegressJob::getExitCode() const {
    if (!_process) {
        return -1;
    }
    return _process->getExitCode();
}

void RegressJob::terminate() {
    if (_process) {
        if (!ProcessUtils::killAllChildren(_process->getPID(), SIGKILL)) {
            spdlog::error("Kill failed for process id {}", _process->getPID());
        }
        _process->terminate();
        _process->wait();
    }
}

void RegressJob::wait() {
    if (_process) {
        _process->wait();
    }
}
