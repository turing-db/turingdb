#include "Command.h"

#include <boost/process.hpp>

#include "FileUtils.h"

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

bool Command::run() {
    if (_workingDir.empty()) {
        _workingDir = files::cwd();
    }

    if (!files::exists(_workingDir)) {
        return false;
    }

    _workingDir = files::abspath(_workingDir);
    if (_workingDir.empty()) {
        return false;
    }

    _returnCode = boost::process::system(_cmd, _args,
                boost::process::start_dir(_workingDir.string()));
    return true;
}
