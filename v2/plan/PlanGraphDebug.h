#pragma once

#include <ostream>

namespace db {
class GraphView;
}

namespace db::v2 {

class PlanGraph;

class PlanGraphDebug {
public:
    static void dumpMermaid(std::ostream& output, const GraphView& view, const PlanGraph& planGraph);
};

}
