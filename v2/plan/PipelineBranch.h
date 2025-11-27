#pragma once

#include <vector>

namespace db::v2 {

class PlanGraphNode;
class PipelineBranches;
class PipelineOutputInterface;

class PipelineBranch {
public:
    friend PipelineBranches;

    struct Input {
        PipelineBranch* _prev {nullptr};
        PipelineOutputInterface* _prevIface {nullptr};
    };

    struct Output {
        PipelineBranch* _next {nullptr};
    };

    using Inputs = std::vector<Input>;
    using Outputs = std::vector<Output>;
    using Nodes = std::vector<PlanGraphNode*>;

    const Inputs& inputs() const { return _inputs; }
    const Outputs& outputs() const { return _outputs; }

    const Nodes& nodes() const { return _nodes; }

    void addNode(PlanGraphNode* node) {
        _nodes.push_back(node);
    }

    void connectTo(PipelineBranch* nextBranch) {
        _outputs.emplace_back(nextBranch);
        nextBranch->_inputs.emplace_back(this);
    }

    bool isSortDiscovered() const { return _sortDiscovered; }
    void setSortDiscovered(bool discovered) { _sortDiscovered = discovered; }

    static PipelineBranch* create(PipelineBranches* branches);

private:
    Nodes _nodes;
    Inputs _inputs;
    Outputs _outputs;
    bool _sortDiscovered {false};

    PipelineBranch();
    ~PipelineBranch();
};

}
