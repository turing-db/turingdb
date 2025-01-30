#include <stdlib.h>
#include <argparse.hpp>

#include "TuringDB.h"

#include "ToolInit.h"

using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("turingdb");

    //auto& argParser = toolInit.getArgParser();

    toolInit.init(argc, argv);

    TuringDB turingDB;

    return EXIT_SUCCESS;
}

