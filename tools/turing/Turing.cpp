#include <stdlib.h>

#include "ToolInit.h"
#include "TuringTool.h"

int main(int argc, const char** argv) {
    ToolInit toolInit("turing");
    toolInit.disableOutputDir();

    TuringTool turingTool(toolInit);
    turingTool.setup();

    toolInit.init(argc, argv);

    turingTool.run();

    return EXIT_SUCCESS;
}
