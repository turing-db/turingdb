#pragma once

#include <vector>

namespace db {

class VariableDependency;
class VariableDependencyGraph;


class VariableDependencyGraphTraversal {
public:
    void getTraversal(const VariableDependencyGraph* graph,
                      std::vector<const VariableDependency*>& traversal);

private:
};

}
