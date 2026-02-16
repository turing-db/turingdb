#include "StopCmd.h"

#include <argparse.hpp>
#include "spdlog/fmt/bundled/base.h"

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
    fmt::print("Stopping TuringDB\n");
    return EXIT_SUCCESS;
}
