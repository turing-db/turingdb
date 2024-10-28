#include <stdlib.h>

#include <argparse.hpp>

#include "PipeUtils.h"

#include "ToolInit.h"

int main(int argc, const char** argv) {
    ToolInit toolInit("simple_server");
    toolInit.disableOutputDir();

    bool startServer = false;
    auto& argparser = toolInit.getArgParser();
    argparser.add_argument("-s")
             .help("Start server")
             .store_into(startServer);

    toolInit.init(argc, argv);

    PipeSample sample("simple_server");

    sample.createSimpleGraph();

    if (startServer) {
        sample.startHttpServer();
    }

    return EXIT_SUCCESS;
}
