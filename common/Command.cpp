#include "Command.h"

#include <ranges>
#include <spdlog/spdlog.h>

#include "FileUtils.h"
#include "log/LogUtils.h"
#include "Process.h"

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

std::unique_ptr<Process> Command::runAsync() {
    
    auto proc = std::make_unique<Process>();
    proc->setWriteStdout(_stdoutEnabled);
    proc->setReadStdin(false);

    if (!generateBashCmd(*proc)) {
        return nullptr;
    }

    if (!proc->startAsync()) {
        return nullptr;
    }

    return proc;
}

bool Command::run() {
    const auto proc = runAsync();
    if (!proc) {
        return false;
    }

    if (!proc->wait()) {
        return false;
    }

    _returnCode = proc->getExitCode();
    return true;
}

void Command::getLogs(std::string& data) const {
    FileUtils::readContent(_logFile, data);
}

bool Command::searchCmd() {
    std::string result;
    FileUtils::findExecutableInPath(_cmd, result);
    _cmd = result;
    return !_cmd.empty();
}

void Command::generateCmdString(std::string& cmdStr) {
    cmdStr = "cd ";
    cmdStr += _workingDir.string();
    cmdStr += "; ";

    for (const auto& envEntry : _env) {
        cmdStr += envEntry.first;
        cmdStr += "='";
        cmdStr += envEntry.second;
        cmdStr += "' ";
    }

    cmdStr += _cmd;
    cmdStr += " ";

    for (const auto& [opt, arg] : _options) {
        cmdStr += " ";
        cmdStr += opt;
        cmdStr += " '";
        cmdStr += arg;
        cmdStr += "'";
    }

    for (const auto& arg : _args) {
        cmdStr += " '";
        cmdStr += arg;
        cmdStr += "'";
    }

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

bool Command::generateBashCmd(Process& proc) {
    // Working directory
    if (_workingDir.empty()) {
        _workingDir = FileUtils::cwd();
    }

    if (!FileUtils::exists(_workingDir)) {
        logt::DirectoryDoesNotExist(_workingDir.string());
        return false;
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
    if (FileUtils::isAbsolute(_cmd) && FileUtils::exists(_cmd)) {
        _cmd = FileUtils::abspath(_cmd);
    } else {
        if (!searchCmd()) {
            logt::ExecutableNotFound(_cmd);
            return false;
        }
    }

    std::string cmdStr;
    generateCmdString(cmdStr);
    proc.setCmd("/bin/bash");

    // Generate command script or direct bash command
    if (_generateScript) {
        if (_scriptPath.empty()) {
            _scriptPath = _workingDir / "cmd.sh";
        }

        cmdStr.push_back('\n');
        if (!FileUtils::writeFile(_scriptPath, cmdStr)) {
            spdlog::error("Failed to write command script {}", _scriptPath.string());
            return false;
        }

        proc.addArg(_scriptPath.string());
    } else {
        proc.addArg("-c");
        proc.addArg(cmdStr);
    }

    return true;
}

void Command::addOption(const std::string& optName, const std::string& arg) {
    _options.emplace_back(optName, arg);
}
