#include <argparse.hpp>

#include "ToolInit.h"
#include "DBServer.h"
#include "DBServerConfig.h"
#include "Demonology.h"

using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("turingdb");

    auto& argParser = toolInit.getArgParser();

    DBServerConfig config;

    bool nodemon = true;
    argParser.add_argument("-nodemon")
        .default_value(true)
        .store_into(nodemon)
        .nargs(0)
        .help("Do not spawn as a background process");

    argParser.add_argument("-p")
        .default_value((uint32_t)6666)
        .store_into(config._port)
        .nargs(1)
        .help("Listening port");

    argParser.add_argument("-j")
        .default_value((uint32_t)8)
        .store_into(config._workerCount)
        .nargs(1)
        .help("Number of threads");

    argParser.add_argument("-c")
        .store_into(config._maxConnections)
        .default_value((uint32_t)1024)
        .nargs(1)
        .help("Maximum number of concurrent connections before starting to close existing ones");

    toolInit.init(argc, argv);

    // Demonize
    if (!nodemon) {
        Demonology::demonize();
    }

    // Database server
    DBServer server(config);
    server.start();

    return EXIT_SUCCESS;
}

