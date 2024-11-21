#include <stdlib.h>

#include <argparse.hpp>

#include "PipeUtils.h"

#include "ToolInit.h"

int main(int argc, const char** argv) {
    ToolInit toolInit("simple_server");
    toolInit.disableOutputDir();

    bool startServer = false;
    size_t threadCount = 8;
    auto& argparser = toolInit.getArgParser();
    argparser.add_argument("-s")
             .help("Start server")
             .store_into(startServer);
    argparser.add_argument("-t")
             .help("Number of server threads")
             .metavar("count")
             .nargs(1)
             .store_into(threadCount);

    toolInit.init(argc, argv);

    PipeSample sample("simple_server");
    sample.getServerConfig()._workerCount = threadCount;

    sample.createSimpleGraph();

    if (startServer) {
        sample.startHttpServer();
    }

    return EXIT_SUCCESS;
}
