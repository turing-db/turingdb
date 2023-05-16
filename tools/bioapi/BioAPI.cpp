#include "ToolInit.h"

#include "APIServer.h"
#include "APIServerConfig.h"

#include "BioLog.h"

using namespace Log;

int main(int argc, const char** argv) {
    ToolInit toolInit("bioapi");

    toolInit.init(argc, argv);

    APIServerConfig config;

    APIServer server(config);
    server.run();

    BioLog::printSummary();
    BioLog::destroy();
    return EXIT_SUCCESS;
}
