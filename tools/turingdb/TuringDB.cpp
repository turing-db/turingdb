#include <signal.h>

#include <spdlog/spdlog.h>

#include "ToolInit.h"
#include "DBServer.h"
#include "DBServerConfig.h"
#include "Demonology.h"

using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("turingdb");

    toolInit.init(argc, argv);

    // Demonize
    Demonology::demonize();

    DBServerConfig dbServerConfig;

    // Database server
    DBServer server(dbServerConfig);
    server.start();

    return EXIT_SUCCESS;
}
