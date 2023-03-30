#include "Command.h"

#include <boost/process.hpp>

#include "FileUtils.h"

#include "BioLog.h"
#include "MsgCommon.h"

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

bool Command::run() {
    // Working directory
    if (_workingDir.empty()) {
        _workingDir = files::cwd();
    }

    if (!files::exists(_workingDir)) {
        BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS()
                    << _workingDir.string());
        return false;
    }

    _workingDir = files::abspath(_workingDir);

    // Stdout and stderr log file
    if (_writeLogFile) {
        if (_logFile.empty()) {
            _logFile = _workingDir/"cmd.log";
        }
        _logFile = files::abspath(_logFile);
    } else {
        _logFile = "/dev/null";
    }

    // Command executable
    if (files::exists(_cmd)) {
        _cmd = files::abspath(_cmd);
    } else {
        if (!searchCmd()) {
            BioLog::log(msg::ERROR_EXECUTABLE_NOT_FOUND() << _cmd);
            return false;
        }
    }

    std::string cmdStr;
    generateCmdString(cmdStr);

    if (_generateScript) {
        if (_scriptPath.empty()) {
            _scriptPath = _workingDir/"cmd.sh";
        }

        cmdStr.push_back('\n');
        if (!files::writeFile(_scriptPath, cmdStr)) {
            BioLog::log(msg::ERROR_FAILED_TO_WRITE_COMMAND_SCRIPT()
                        << _scriptPath.string());
            return false;
        }

        const std::string scriptCmd = "sh " + _scriptPath.string();
        _returnCode = system(scriptCmd.c_str());
    } else {
        _returnCode = system(cmdStr.c_str());
    }

    return true;
}

bool Command::searchCmd() {
    const auto bpExecPath = boost::process::search_path(_cmd);
    if (bpExecPath.empty()) {
        return false;
    }

    _cmd = bpExecPath.string();

    return true;
}

void Command::generateCmdString(std::string& cmdStr) {
    cmdStr = "cd '";
    cmdStr += _workingDir.string();
    cmdStr += "' && ";
    cmdStr += _cmd;
    cmdStr += " ";
    
    for (const auto& arg : _args) {
        cmdStr += " '";
        cmdStr += arg;
        cmdStr += "'";
    }

    cmdStr += " > '";
    cmdStr += _logFile.string();
    cmdStr += "' 2>&1";
}
