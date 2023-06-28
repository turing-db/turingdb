#include "ToolInit.h"

#include "APIServer.h"

#include "BioLog.h"

using namespace Log;

int main(int argc, const char** argv) {
    ToolInit toolInit("bioserver");

    toolInit.init(argc, argv);

    APIServer server;
    server.run();

    BioLog::printSummary();
    BioLog::destroy();
    return EXIT_SUCCESS;
}
