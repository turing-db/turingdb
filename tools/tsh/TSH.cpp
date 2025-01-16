#include <string>

#include <argparse.hpp>

#include "ToolInit.h"
#include "TuringShell.h"

int main(int argc, const char** argv) {
    ToolInit toolInit("tsh");
    toolInit.disableOutputDir();

    std::string graphName;
    short unsigned portNum = 6666;
    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("-graph")
             .help("Graph name")
             .nargs(1)
             .store_into(graphName);

    argParser.add_argument("-p")
             .help("TuringDB server port")
             .nargs(1)
             .scan<'d', short unsigned>()
             .store_into(portNum);     

    toolInit.init(argc, argv);

    TuringShell shell;
    shell.setPort(portNum);

    if (!graphName.empty()) {
        shell.setGraphName(graphName);
    }
    
    shell.startLoop();

    return EXIT_SUCCESS;
}
