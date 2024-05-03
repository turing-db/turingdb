#include "RegressJob.h"

#include <signal.h>
#include <boost/process.hpp>
#include <spdlog/spdlog.h>

#include "Command.h"
#include "ProcessUtils.h"
#include "LogUtils.h"

RegressJob::RegressJob(const Path& path)
    : _path(path)
{
}

RegressJob::~RegressJob() {
}

bool RegressJob::start(const ProcessGroup& group) {
    const auto runScriptPath = _path/"run.sh";
    if (!FileUtils::exists(runScriptPath)) {
        logt::FileNotFound(runScriptPath.string());
        return false;
    }

    Command cmd(runScriptPath.string());
    cmd.setWorkingDir(_path);
    cmd.setLogFile(_path/"run.log");
    cmd.setGenerateScript(true);

    spdlog::info("Run: {}", _path.string());
    _process = cmd.runAsync(group);
    return true;
}

bool RegressJob::isRunning() {
    if (!_process) {
        return false;
    }
    return _process->running();
}

int RegressJob::getExitCode() const {
    if (!_process) {
        return -1;
    }
    return _process->exit_code();
}

void RegressJob::terminate() {
    if (_process) {
        if (!ProcessUtils::killAllChildren(_process->id(), SIGKILL)) {
            spdlog::error("Kill failed for process id {}", _process->id());
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
