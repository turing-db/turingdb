#include "Command.h"

#include <boost/process.hpp>

#include "FileUtils.h"
#include "BioLog.h"
#include "MsgCommon.h"
#include "MsgDB.h"

using namespace Log;

Command::Command(const std::string& cmd)
    : _cmd(cmd)
{
}

Command::~Command() {
}

void Command::addArg(const std::string& arg) {
    _args.emplace_back(arg);
}

void Command::setWorkingDir(const Path& path) {
    _workingDir = path;
}

void Command::setLogFile(const Path& path) {
    _logFile = path;
}

void Command::setScriptPath(const Path& path) {
    _scriptPath = path;
}

void Command::setEnvVar(const std::string& var, const std::string& value) {
    _env.emplace_back(var, value);
}

bool Command::run() {
    std::string bashCmd;
    getBashCmd(bashCmd);

    if (bashCmd.empty()) {
        return false;
    }

    if (_verbose) {
        BioLog::echo("Executing command "+bashCmd);
    }
    _returnCode = system(bashCmd.c_str());
    return true;
}

ProcessChild Command::runAsync(ProcessGroup& group) {
    std::string bashCmd;
    getBashCmd(bashCmd, true);

    if (_writeLogFile) {
        return std::make_unique<boost::process::child>(
            bashCmd,
            boost::process::std_in.close(),
            boost::process::std_out > _logFile.string(),
            boost::process::std_err > _logFile.string(),
            *group);
    }

    return std::make_unique<boost::process::child>(
        bashCmd,
        boost::process::std_in.close(),
        boost::process::std_out > boost::process::null,
        boost::process::std_err > boost::process::null);
}

void Command::getLogs(std::string& data) const {
    FileUtils::readContent(_logFile, data);
}

bool Command::searchCmd() {
    const auto bpExecPath = boost::process::search_path(_cmd);
    if (bpExecPath.empty()) {
        return false;
    }

    _cmd = bpExecPath.string();

    return true;
}

void Command::generateCmdString(std::string& cmdStr, bool async) {
    cmdStr = "cd '";
    cmdStr += _workingDir.string();
    cmdStr += "'; ";

    for (const auto& envEntry : _env) {
        cmdStr += envEntry.first;
        cmdStr += "='";
        cmdStr += envEntry.second;
        cmdStr += "' ";
    }

    cmdStr += _cmd;
    cmdStr += " ";

    for (const auto& arg : _args) {
        cmdStr += " '";
        cmdStr += arg;
        cmdStr += "'";
    }

    if (async)
        return;

    if (_stdoutEnabled) {
        cmdStr += " 2>&1 | tee '";
        cmdStr += _logFile.string();
        cmdStr += "'";
    } else {
        cmdStr += " > '";
        cmdStr += _logFile.string();
        cmdStr += "' 2>&1";
    }

    cmdStr += "; exit ${PIPESTATUS[0]}";
}

void Command::getBashCmd(std::string& bashCmd, bool async) {
    bashCmd = "";

    // Working directory
    if (_workingDir.empty()) {
        _workingDir = FileUtils::cwd();
    }

    if (!FileUtils::exists(_workingDir)) {
        BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS()
                    << _workingDir.string());
        return;
    }

    _workingDir = FileUtils::abspath(_workingDir);

    // Stdout and stderr log file
    if (_writeLogFile) {
        if (_logFile.empty()) {
            _logFile = _workingDir / "cmd.log";
        }
        _logFile = FileUtils::abspath(_logFile);
    } else {
        _logFile = "/dev/null";
    }

    // Command executable
    if (FileUtils::exists(_cmd)) {
        _cmd = FileUtils::abspath(_cmd);
    } else {
        if (!searchCmd()) {
            BioLog::log(msg::ERROR_EXECUTABLE_NOT_FOUND() << _cmd);
            return;
        }
    }

    std::string cmdStr;
    generateCmdString(cmdStr, async);
    bashCmd = "bash ";

    if (_generateScript) {
        if (_scriptPath.empty()) {
            _scriptPath = _workingDir / "cmd.sh";
        }

        cmdStr.push_back('\n');
        if (!FileUtils::writeFile(_scriptPath, cmdStr)) {
            BioLog::log(msg::ERROR_FAILED_TO_WRITE_COMMAND_SCRIPT()
                        << _scriptPath.string());
            return;
        }

        bashCmd += _scriptPath.string();
    } else {
        bashCmd += "-c '" + cmdStr + "'";
    }
}
