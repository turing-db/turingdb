#include "PipelineDumper.h"

#include <unordered_map>

#include <spdlog/fmt/fmt.h>

#include "PipelineV2.h"
#include "Processor.h"

using namespace db::v2;

void PipelineDumper::dumpMermaid(std::ostream& out) {
    out << R"(
%%{ init: {"theme": "default",
           "themeVariables": { "wrap": "false" },
           "flowchart": { "curve": "linear",
                          "markdownAutoWrap":"false",
                          "wrappingWidth": "1000" }
           }
}%%
)";

    out << "flowchart TD\n";

    std::unordered_map<const Processor*, size_t> procIndex;

    const auto& processors = _pipeline->processors();

    for (size_t i = 0; i < processors.size(); i++) {
        const Processor* proc = processors[i];
        procIndex[proc] = i;

        out << fmt::format("    {}[\"`\n", i);
        out << fmt::format("        __{}__\n", proc->getName());
        out << "    `\"]\n";
    }

    for (const Processor* proc : processors) {
        const size_t src = procIndex.at(proc);
        for (const PipelineOutputPort* port : proc->outputs()) {
            const Processor* next = port->getConnectedPort()->getProcessor();
            const size_t target = procIndex.at(next);
            out << fmt::format("    {}-->{}\n", src, target);
        }
    }
}
