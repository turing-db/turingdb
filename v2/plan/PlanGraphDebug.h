#pragma once

#include <ostream>

namespace db {

class GraphView;
class PlanGraph;

class PlanGraphDebug {
public:
    static void dumpMermaid(std::ostream& output, const GraphView& view, const PlanGraph& planGraph);
};

}
