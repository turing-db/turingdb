#include "ToolInit.h"

#include "TuringUIServer.h"

#include "BioLog.h"

#define BIOCAULDRON_TOOL_NAME     "cauldron"

using namespace ui;
using namespace Log;

int main(int argc, const char** argv) {
    ToolInit toolInit(BIOCAULDRON_TOOL_NAME);
    toolInit.init();

    TuringUIServer server(toolInit.getOutputsDir());
    server.start();

    BioLog::printSummary();
    BioLog::destroy();
    return EXIT_SUCCESS;
}
