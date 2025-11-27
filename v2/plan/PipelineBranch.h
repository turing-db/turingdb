#pragma once

#include <vector>

namespace db::v2 {

class PlanGraphNode;
class PipelineBranches;

class PipelineBranch {
public:
    friend PipelineBranches;
    using Nodes = std::vector<PlanGraphNode*>;
    using Branches = std::vector<PipelineBranch*>;

    const Nodes& nodes() const { return _nodes; }

    const Branches& next() const { return _next; }

    static PipelineBranch* create(PipelineBranches* branches);

    void addNode(PlanGraphNode* node) {
        _nodes.push_back(node);
    }

    void connectTo(PipelineBranch* nextBranch) {
        _next.push_back(nextBranch);
    }

    bool isSortDiscovered() const { return _sortDiscovered; }
    void setSortDiscovered(bool discovered) { _sortDiscovered = discovered; }

private:
    Nodes _nodes;
    Branches _next;
    bool _sortDiscovered {false};

    PipelineBranch();
    ~PipelineBranch();
};

}
