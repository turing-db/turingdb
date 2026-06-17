#pragma once

#include <iosfwd>

namespace db {

class VariableDependencyGraph;

class VariableDependencyGraphDumper {
public:
    static void dumpMermaid(const VariableDependencyGraph& graph, std::ostream& out);
};

}
