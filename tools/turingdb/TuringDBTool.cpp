#include <stdlib.h>
#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "TuringDB.h"
#include "LocalMemory.h"
#include "TuringShell.h"
#include "TuringServer.h"
#include "DBServerConfig.h"
#include "TuringConfig.h"
#include "Demonology.h"

#include "ToolInit.h"
#include "TuringException.h"

using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("turingdb");

    auto& argParser = toolInit.getArgParser();

    bool demonize = false;
    bool inMemory = false;
    bool resetDefault = false;
    unsigned port = 6666;
    std::string address {"127.0.0.1"};
    std::string turingDir;
    std::vector<std::string> graphsToLoad;

    argParser.add_argument("-p")
             .metavar("port")
             .help("Server listen port")
             .store_into(port);
    argParser.add_argument("-i")
             .metavar("addr")
             .help("Server listen address (localhost by default)") 
             .store_into(address);
    argParser.add_argument("-demon")
             .help("Launch TuringDB as a daemon in the background")
             .store_into(demonize);
    argParser.add_argument("-reset-default")
             .help("Reset the content of the default graph")
             .store_into(resetDefault);
    argParser.add_argument("-load")
             .metavar("graph_name")
             .help("Load a graph at startup")
             .store_into(graphsToLoad);
    argParser.add_argument("-in-memory")
             .help("Run turingdb in-memory only without writing graphs on disk")
             .store_into(inMemory);
    argParser.add_argument("-turing-dir")
             .metavar("path")
             .store_into(turingDir)
             .help("Root Turing directory");

    toolInit.init(argc, argv);

    // Config
    TuringConfig config;
    config.setSyncedOnDisk(!inMemory);

    if (!turingDir.empty()) {
        fs::Path absTuringDir(turingDir);
        if (!absTuringDir.toAbsolute()) {
            spdlog::error("Failed to get absolute path of turing directory {}",
                          turingDir);
            return EXIT_FAILURE;
        }
        config.setTuringDirectory(absTuringDir);
    }

    spdlog::info("TuringDB path: {}", config.getTuringDir().get());

    // Delete existing `default` graph if requested
    if (resetDefault) {
        spdlog::info("Resetting default graph.");

        spdlog::info("Searching for default in {}.", config.getGraphsDir().get());
        const fs::Path defaultGraphPath = config.getGraphsDir() / "default";
        if (defaultGraphPath.exists()) {
            defaultGraphPath.rm();
            spdlog::info("Default graph deleted.");
        } else {
            spdlog::warn("Default graph not found.");
        }
    }

    if (demonize) {
        Demonology::demonize();
    }

    try {
        // Run TuringDB
        LocalMemory mem;
        TuringDB turingDB(&config);
        turingDB.init();

        // Load graphs
        for (const auto& graphName : graphsToLoad) {
            const auto res = turingDB.query("load graph " + graphName, "", &mem);
            if (!res.isOk()) {
                spdlog::error("Failed to load graph {}: {}", graphName, res.getError());
                return EXIT_FAILURE;
            }
        }

        // Server
        DBServerConfig serverConfig;
        serverConfig.setPort(port);
        serverConfig.setAddress(address);

        TuringServer server(serverConfig, turingDB);
        server.start();

        // CLI Shell
        if (demonize) {
            server.wait();
            server.stop();
        } else {
            TuringShell shell(turingDB, &mem);

            if (!graphsToLoad.empty()) {
                shell.setGraphName(graphsToLoad.front());
            }

            shell.startLoop();
            server.stop();
        }
    } catch (TuringException& e) {
        spdlog::error("{}", e.what());
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

