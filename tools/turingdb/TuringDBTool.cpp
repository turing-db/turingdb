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

using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("turingdb");

    auto& argParser = toolInit.getArgParser();

    bool noServer = false;
    bool nodemon = false;
    bool inMemory = true;
    unsigned port = 6666;
    std::string address {"127.0.0.1"};
    std::string turingDir;
    std::vector<std::string> graphsToLoad;

    argParser.add_argument("-cli")
             .store_into(noServer);
    argParser.add_argument("-p")
             .store_into(port);
    argParser.add_argument("-i")
             .store_into(address);
    argParser.add_argument("-nodemon")
             .store_into(nodemon);
    argParser.add_argument("-load")
             .store_into(graphsToLoad);
    argParser.add_argument("-in-memory")
             .store_into(inMemory);
    argParser.add_argument("-turing-dir")
             .store_into(turingDir)
             .help("Root Turing directory");

    toolInit.init(argc, argv);

    if (!noServer && !nodemon) {
        Demonology::demonize();
    }

    // Config
    TuringConfig config = TuringConfig::createDefault();
    config.setSyncedOnDisk(!inMemory);

    fmt::println("TuringDB path: {}", config.getTuringDir().get());
    if (!turingDir.empty()) {
        config.setTuringDirectory(fs::Path {turingDir});
    }

    // Run TuringDB
    TuringDB turingDB(&config);
    turingDB.run();

    // Load graphs
    LocalMemory mem;

    for (const auto& graphName : graphsToLoad) {
        const auto res = turingDB.query("load graph " + graphName, "", &mem);
        if (!res.isOk()) {
            spdlog::error("Failed to load graph {}: {}", graphName, res.getError());
            return EXIT_FAILURE;
        }
    }

    if (noServer) {
        TuringShell shell(turingDB, &mem);

        if (!graphsToLoad.empty()) {
            shell.setGraphName(graphsToLoad.front());
        }

        shell.startLoop();
    } else {
        DBServerConfig config;
        config.setPort(port);
        config.setAddress(address);

        TuringServer server(config, turingDB);
        server.start();
    }

    return EXIT_SUCCESS;
}

