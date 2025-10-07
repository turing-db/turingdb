#include "PipelineDebug.h"

#include "PipelineV2.h"
#include "Processor.h"

using namespace db::v2;

void PipelineDebug::dumpMermaid(std::ostream& output, PipelineV2* pipeline) {
    output << "---\n";
    output << "config:\n";
    output << "  layout: hierarchical\n";
    output << "---\n";
    output << "erDiagram\n";

    for (const Processor* processor : pipeline->processors()) {
        output << fmt::format("    {}[{}] {{\n", fmt::ptr(processor), processor->getName());

        output << "    }\n";

        // Writing connections
        for (const PipelinePort* out : processor->outputs()) {
            const PipelinePort* connectedPort = out->getConnectedPort();
            if (connectedPort) {
                output << fmt::format("    {} ||--o{{ {} : \" \"\n", fmt::ptr(processor), fmt::ptr(connectedPort->getProcessor()));
            }
        }
    }
}
