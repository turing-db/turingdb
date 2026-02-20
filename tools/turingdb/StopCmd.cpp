#include "StopCmd.h"

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "TuringConfig.h"
#include "LockFile.h"
#include "SystemEventHandler.h"
#include "LogSetup.h"

using namespace db;

StopCmd::StopCmd()
    : _argParser("stop")
{
}

StopCmd::~StopCmd() = default;

int StopCmd::execute() {
    LogSetup::setupLogConsole();

    TuringConfig config;

    if (!_turingDir.empty()) {
        config.setTuringDirectory(fs::Path {_turingDir});
    }

    _turingDir = config.getTuringDir().get();

    const fs::Path socketPath = config.getSocketPath();
    if (!socketPath.exists()) {
        spdlog::error("No TuringDB instance seems to be running at {}", _turingDir);
        return EXIT_FAILURE;
    }

    if (!SystemEventHandler::requestStop(socketPath)) {
        spdlog::error("Could not stop the TuringDB instance at {}", _turingDir);
        return EXIT_FAILURE;
    }

    LockFile lockFile;
    lockFile.setPath(config.getLockFilePath());

    if (!lockFile.waitUnlock(_timeout)) {
        spdlog::error("TuringDB at {} did not stop within {} ms", _turingDir, _timeout);
        return EXIT_FAILURE;
    }

    spdlog::info("Stopped TuringDB instance at {}", _turingDir);

    return EXIT_SUCCESS;
}

std::unique_ptr<StopCmd> StopCmd::create() {
    std::unique_ptr<StopCmd> cmd {new StopCmd()};

    cmd->initialize();

    return cmd;
}

void StopCmd::initialize() {
    _argParser.add_description("Stops the TuringDB server");

    _argParser.add_argument("-turing-dir")
        .metavar("path")
        .help("Root Turing directory")
        .store_into(_turingDir);
    _argParser.add_argument("-timeout", "-t")
        .metavar("ms")
        .help("Milliseconds to wait for the process to release its lock (default: 3000)")
        .store_into(_timeout);
}
