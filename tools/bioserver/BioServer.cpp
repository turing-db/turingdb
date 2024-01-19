#include "ToolInit.h"

#include "DBServer.h"
#include "DBServerConfig.h"

#include "BioLog.h"

using namespace Log;

int main(int argc, const char** argv) {
    ToolInit toolInit("bioserver");

    toolInit.init(argc, argv);

    // Configuration of the DB Server
    DBServerConfig dbServerConfig;

    // Database server
    DBServer server(dbServerConfig);
    server.run();

    BioLog::printSummary();
    BioLog::destroy();
    return EXIT_SUCCESS;
}
