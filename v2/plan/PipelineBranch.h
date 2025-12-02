#pragma once

#include <vector>

namespace db::v2 {

class PlanGraphNode;
class PipelineBranches;
class PipelineOutputInterface;

class PipelineBranch {
public:
    friend PipelineBranches;

    using Inputs = std::vector<PipelineBranch*>;
    using Outputs = std::vector<PipelineBranch*>;
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

    PipelineOutputInterface* getOutputInterface() const { return _outIface; }
    void setOutputInterface(PipelineOutputInterface* iface) { _outIface = iface; }

    static PipelineBranch* create(PipelineBranches* branches);

private:
    Nodes _nodes;
    Inputs _inputs;
    Outputs _outputs;
    PipelineOutputInterface* _outIface {nullptr};
    bool _sortDiscovered {false};

    PipelineBranch();
    ~PipelineBranch();
};

}
