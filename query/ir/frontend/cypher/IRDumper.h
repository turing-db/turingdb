#pragma once

#include <iosfwd>

namespace db {

class VariableDependencyGraph;

class IRDumper {
public:
    static void dumpMermaid(const VariableDependencyGraph& graph, std::ostream& out);
};

}
