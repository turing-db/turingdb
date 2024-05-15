#include <signal.h>

#include <spdlog/spdlog.h>

#include "ToolInit.h"
#include "DBServer.h"
#include "DBServerConfig.h"
#include "Demonology.h"
#include "ProcessUtils.h"

using namespace db;

void signalHandler(int sig) {
    spdlog::info("Server received signal {}, terminating", sig);
    exit(EXIT_SUCCESS);
}

int main(int argc, const char** argv) {
    ToolInit toolInit("turingdb");

    toolInit.init(argc, argv);

    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // Demonize
    Demonology::demonize();

    // Write PID file
    const auto pidFilePath = toolInit.getOutputsDirPath()/"pid";
    if (!ProcessUtils::writePIDFile(pidFilePath)) {
        spdlog::error("Failed to write PID file {}", pidFilePath.string());
        exit(EXIT_FAILURE);
    }


    DBServerConfig dbServerConfig;

    // Database server
    DBServer server(dbServerConfig);
    server.start();

    return EXIT_SUCCESS;
}
