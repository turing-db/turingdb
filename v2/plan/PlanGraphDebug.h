#pragma once

#include <string>

namespace db {

class GraphView;
class PlanGraph;

class PlanGraphDebug {
public:
    static std::string dumpMermaid(const GraphView& view, const PlanGraph& planGraph);
};

}
