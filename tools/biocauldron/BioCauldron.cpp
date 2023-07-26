#include "ToolInit.h"

#include "TuringUIServer.h"

#include "BioLog.h"
#include "MsgUIServer.h"

#define BIOCAULDRON_TOOL_NAME     "biocauldron"

using namespace ui;
using namespace Log;

int main(int argc, const char** argv) {
    ToolInit toolInit(BIOCAULDRON_TOOL_NAME);
    toolInit.init(argc, argv);

    TuringUIServer server(toolInit.getOutputsDir());
    server.setCleanEnabled(false);
    int code = server.start();

    if (code != 0) {
        std::string logData;
        server.getLogs(logData);
        BioLog::log(msg::INFO_UI_SERVER_OUTPUT() << logData);
        BioLog::printSummary();
        BioLog::destroy();
        return EXIT_FAILURE;
    }

    BioLog::printSummary();
    BioLog::destroy();
    return EXIT_SUCCESS;
}
