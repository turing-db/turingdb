#include "Command.h"

#include <boost/process.hpp>
#include <spdlog/spdlog.h>

#include "FileUtils.h"
#include "LogUtils.h"
#include "BioAssert.h"

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

ProcessChild Command::runAsync(const ProcessGroup& group) {
    return createProcess(group);
}

bool Command::run() {
    auto group = std::make_unique<boost::process::group>();
    auto child = createProcess(group);
    if (!child) {
        return false;
    }

    child->wait();
    _returnCode = child->exit_code();
    return true;
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

ProcessChild Command::createProcess(const ProcessGroup& group) {
    bioassert(group);
    std::string bashCmd;
    if (!getBashCmd(bashCmd)) {
        return ProcessChild();
    }

    if (_stdoutEnabled) {
        return std::make_unique<boost::process::child>(
            bashCmd,
            boost::process::std_in.close(),
            *group);
    }

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
        boost::process::std_err > boost::process::null,
        *group);
}

void Command::generateCmdString(std::string& cmdStr) {
    cmdStr = "cd ";
    cmdStr += _workingDir.string();
    cmdStr += "; ";

    for (const auto& envEntry : _env) {
        cmdStr += "'";
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

bool Command::getBashCmd(std::string& bashCmd) {
    bashCmd = "";

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
    if (FileUtils::exists(_cmd)) {
        _cmd = FileUtils::abspath(_cmd);
    } else {
        if (!searchCmd()) {
            logt::ExecutableNotFound(_cmd);
            return false;
        }
    }

    std::string cmdStr;
    generateCmdString(cmdStr);
    bashCmd = "bash ";

    if (_generateScript) {
        if (_scriptPath.empty()) {
            _scriptPath = _workingDir / "cmd.sh";
        }

        cmdStr.push_back('\n');
        if (!FileUtils::writeFile(_scriptPath, cmdStr)) {
            spdlog::error("Failed to write command script {}", _scriptPath.string());
            return false;
        }

        bashCmd += _scriptPath.string();
    } else {
        bashCmd += "-c \"" + cmdStr + "\"";
    }

    return true;
}
