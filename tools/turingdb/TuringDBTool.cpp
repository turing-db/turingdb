#include <stdlib.h>
#include <argparse.hpp>

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
    argParser.add_argument("-cli")
             .store_into(noServer);
    argParser.add_argument("-p")
             .store_into(port);
    argParser.add_argument("-nodemon")
             .store_into(nodemon);

    toolInit.init(argc, argv);

    TuringDB turingDB;
    LocalMemory mem;

    if (noServer) {
        TuringShell shell(turingDB, &mem);
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

