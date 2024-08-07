#include <signal.h>

#include <spdlog/spdlog.h>
#include <argparse.hpp>

#include "ToolInit.h"
#include "DBServer.h"
#include "DBServerConfig.h"
#include "Demonology.h"

void signalHandler(int signum) {
    spdlog::info("Server received signal {}, terminating", signum);
    exit(EXIT_SUCCESS);
}

int main(int argc, const char** argv) {
    ToolInit toolInit("bioserver");

    std::vector<std::string> dbNames;
    bool noDemonRequested = false;

    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("-db")
             .help("Load a database at the start")
             .nargs(1)
             .append()
             .metavar("dbname")
             .store_into(dbNames);

    argParser.add_argument("-nodemon")
             .help("Do not spawn the server in the background as a daemon")
             .store_into(noDemonRequested);

    toolInit.init(argc, argv);

    // Demonize
    if (!noDemonRequested) {
        Demonology::demonize();
    }

    // Install signal handler to handle ctrl+C
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // Configuration of the DB Server
    DBServerConfig dbServerConfig;

    // Database server
    DBServer server(dbServerConfig);

    spdlog::info("Server starting");
    if (!server.run(dbNames)) {
        spdlog::error("Database server terminated with an error");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
