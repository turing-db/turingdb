#pragma once

#include <vector>

namespace db::v2 {

class PlanGraphNode;
class PlanBranches;

class PlanBranch {
public:
    friend PlanBranches;
    using Nodes = std::vector<PlanGraphNode*>;
    using Branches = std::vector<PlanBranch*>;

    const Nodes& nodes() const { return _nodes; }

    const Branches& next() const { return _next; }

    static PlanBranch* create(PlanBranches* branches);

    void addNode(PlanGraphNode* node) {
        _nodes.push_back(node);
    }

    void connectTo(PlanBranch* nextBranch) {
        _next.push_back(nextBranch);
    }

private:
    Nodes _nodes;
    Branches _next;

    PlanBranch();
    ~PlanBranch();
};

}
