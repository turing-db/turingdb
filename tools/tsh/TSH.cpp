#include <string>

#include <argparse.hpp>

#include "ToolInit.h"
#include "TuringShell.h"

int main(int argc, const char** argv) {
    ToolInit toolInit("tsh");
    toolInit.disableOutputDir();

    std::string dbName;
    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("-db")
             .help("Database name")
             .nargs(1)
             .store_into(dbName);        

    toolInit.init(argc, argv);

    TuringShell shell;

    if (!dbName.empty()) {
        shell.setDBName(dbName);
    }
    
    shell.startLoop();

    return EXIT_SUCCESS;
}
