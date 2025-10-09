#pragma once

#include <ostream>

namespace db {
class GraphView;
}

namespace db::v2 {

class PlanGraph;

class PlanGraphDebug {
public:
    static void dumpMermaidConfig(std::ostream& output);
    static void dumpMermaidContent(std::ostream& output, const GraphView& view, const PlanGraph& planGraph);
    static void dumpMermaid(std::ostream& output, const GraphView& view, const PlanGraph& planGraph);
};

}
