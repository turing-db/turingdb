#include <stdlib.h>
#include <argparse.hpp>
#include <spdlog/spdlog.h>
#include <string>
#include <unordered_set>

#include "StartCmd.h"
#include "StopCmd.h"

#include "FatalException.h"

using namespace db;

int main(int argc, const char** argv) {
    const std::unordered_set<std::string_view> subcommands = {
        "start",
        "stop",
    };

    const std::unordered_set<std::string_view> passthrough = {
        "-h", "--help",
        "-v", "--version",
    };

    std::vector<const char*> args(argv, argv + argc);

    // If no explicit subcommand or help/version flag was given, fallback to "start"
    const bool skipInject = argc >= 2 && passthrough.contains(argv[1]);

    if (!skipInject && (argc < 2 || !subcommands.contains(argv[1]))) {
        args.insert(args.begin() + 1, "start");
    }

    argparse::ArgumentParser rootParser("turingdb");

    std::unique_ptr<StartCmd> startCmd = StartCmd::create();
    std::unique_ptr<StopCmd> stopCmd = StopCmd::create();

    rootParser.add_subparser(startCmd->getArgParser());
    rootParser.add_subparser(stopCmd->getArgParser());

    try {
        rootParser.parse_args(args.size(), args.data());

        if (rootParser.is_subcommand_used("start")) {
            return startCmd->execute();
        } else if (rootParser.is_subcommand_used("stop")) {
            return stopCmd->execute();
        } else {
            // Should never happen since at least 'start' is present
            throw FatalException("No subcommand given.");
        }
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << rootParser;
        return EXIT_FAILURE;
    }

    return EXIT_FAILURE;
}
