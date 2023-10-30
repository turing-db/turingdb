#include "RegressJob.h"

#include <signal.h>
#include <boost/process.hpp>

#include "Command.h"
#include "ProcessUtils.h"

#include "BioLog.h"
#include "MsgCommon.h"

using namespace Log;

RegressJob::RegressJob(const Path& path)
    : _path(path)
{
}

RegressJob::~RegressJob() {
}

bool RegressJob::start(ProcessGroup& group) {
    const auto runScriptPath = _path/"run.sh";
    if (!FileUtils::exists(runScriptPath)) {
        BioLog::log(msg::ERROR_FILE_NOT_EXISTS() << runScriptPath.string());
        return false;
    }

    Command cmd(runScriptPath.string());
    cmd.setWorkingDir(_path);
    cmd.setLogFile(_path/"run.log");
    cmd.setGenerateScript(true);

    BioLog::echo("Run: "+_path.string());
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
            BioLog::echo("Kill failed");
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
