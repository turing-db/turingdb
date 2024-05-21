#include "ToolInit.h"

#include <vector>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "ModelBuilder.h"

using namespace mind;

namespace {

bool extractModuleAndClass(const std::string& arg,
                           std::string& moduleName,
                           std::string& className) {
    std::vector<std::string> result;
    boost::split(result, arg, boost::is_any_of(":"));
    if (result.size() != 2) {
        return false;
    }

    moduleName = result[0];
    className = result[1];
    return true;
}

}

int main(int argc, const char** argv) {
    ToolInit toolInit("biollm");

    std::string modelArg;
    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("model")
             .help("Python module and class of the model, must be of the form model:Model")
             .nargs(1)
             .store_into(modelArg);

    toolInit.init(argc, argv);

    std::string moduleName;
    std::string className;
    if (!extractModuleAndClass(modelArg, moduleName, className)) {
        spdlog::error("Can not extract python module and class name from argument '{}'", modelArg);
        return EXIT_FAILURE;
    }

    ModelBuilder modelBuilder(toolInit.getOutputsDir());
    if (!modelBuilder.build(moduleName, className)) {
        spdlog::error("Building LLM scripts failed");
        return EXIT_FAILURE;
    }

    return EXIT_FAILURE;
}


