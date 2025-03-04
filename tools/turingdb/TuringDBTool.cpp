#include <stdlib.h>
#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "TuringDB.h"
#include "LocalMemory.h"
#include "TuringShell.h"
#include "TuringServer.h"
#include "DBServerConfig.h"
#include "Demonology.h"

#include "ToolInit.h"

using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("turingdb");

    auto& argParser = toolInit.getArgParser();

    bool noServer = false;
    unsigned port = 0;
    bool nodemon = false;
    std::vector<std::string> graphsToLoad;
    argParser.add_argument("-cli")
             .store_into(noServer);
    argParser.add_argument("-p")
             .store_into(port);
    argParser.add_argument("-nodemon")
             .store_into(nodemon);
    argParser.add_argument("-load")
             .store_into(graphsToLoad);

    toolInit.init(argc, argv);

    TuringDB turingDB;
    LocalMemory mem;

    for (const auto& graphName : graphsToLoad) {
        const auto res = turingDB.query("load graph " + graphName, "", &mem);
        if (!res.isOk()) {
            spdlog::error("Failed to load graph {}", graphName);
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

        if (!nodemon) {
            Demonology::demonize();
        }

        TuringServer server(config, turingDB);
        server.start();
    }

    return EXIT_SUCCESS;
}

