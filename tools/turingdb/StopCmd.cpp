#include "StopCmd.h"

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "TuringConfig.h"
#include "SystemEventHandler.h"

using namespace db;

StopCmd::StopCmd()
{
    _argParser = std::make_unique<argparse::ArgumentParser>("stop");
    _argParser->add_description("Stops the TuringDB server");
    _argParser->add_argument("-turing-dir")
        .metavar("path")
        .help("Root Turing directory")
        .store_into(_turingDir);
}

StopCmd::~StopCmd() = default;

int StopCmd::execute() {
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
        spdlog::error("Could not stop the TuringDB instance at ", _turingDir);
        return EXIT_FAILURE;
    }

    spdlog::info("Stopped TuringDB instance at {}", _turingDir);

    return EXIT_SUCCESS;
}
