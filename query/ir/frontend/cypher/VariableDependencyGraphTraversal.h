#pragma once

#include <vector>

namespace db {

class VariableDependency;
class VariableDependencyGraph;
class DependencyEdge;

class VariableDependencyGraphTraversal {
public:
    void getTraversal(const VariableDependencyGraph* graph,
                      std::vector<const VariableDependency*>& traversal);

    void edgeTraversal(const VariableDependencyGraph* graph,
                       std::vector<const DependencyEdge*>& traversal);

private:
};

}
