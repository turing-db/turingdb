#include "ToolInit.h"

#include <vector>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include "ModelBuilder.h"

#include "BioLog.h"
#include "MsgLLM.h"
#include "PerfStat.h"

using namespace mind;
using namespace Log;

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

    ArgParser& argParser = toolInit.getArgParser();
    argParser.setArgsDesc("model:Model");

    toolInit.init(argc, argv);

    const auto& args = argParser.args();
    if (args.empty()) {
        return EXIT_FAILURE;
    }

    std::string moduleName;
    std::string className;
    if (!extractModuleAndClass(args.front(), moduleName, className)) {
        return EXIT_FAILURE;
    }

    ModelBuilder modelBuilder(toolInit.getOutputsDir());
    if (!modelBuilder.build(moduleName, className)) {
        BioLog::log(msg::ERROR_LLM_MODEL_BUILDER_FAILED());
        return EXIT_FAILURE;
    }

    BioLog::log(msg::INFO_LLM_MODEL_API_GENERATION_COMPLETE()
                << toolInit.getOutputsDir());

    return EXIT_FAILURE;
}
