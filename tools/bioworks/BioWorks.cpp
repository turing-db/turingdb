#include "ToolInit.h"

#include "WorkflowServer.h"
#include "WorkflowServerConfig.h"

#include "BioLog.h"

using namespace Log;
using namespace workflow;

int main(int argc, const char** argv) {
    ToolInit toolInit("workflow");

    toolInit.init(argc, argv);

    WorkflowServerConfig serverConfig;

    WorkflowServer server(serverConfig);
    server.start();

    BioLog::printSummary();
    BioLog::destroy();
    return EXIT_SUCCESS;
}
