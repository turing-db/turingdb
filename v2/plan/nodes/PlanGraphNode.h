#pragma once

#include "EnumToString.h"
#include "PlanGraphBranch.h"

#include <vector>

namespace db::v2 {

enum class PlanGraphOpcode {
    UNKNOWN,
    VAR,
    SCAN_NODES,
    FILTER_NODE,
    FILTER_EDGE,
    GET_OUT_EDGES,
    GET_IN_EDGES,
    GET_EDGES,
    GET_EDGE_TARGET,
    CREATE_NODE,
    CREATE_EDGE,
    CREATE_GRAPH,
    PROJECT_RESULTS,
    CARTESIAN_PRODUCT,
    PRODUCE_RESULTS,
    _SIZE
};

using PlanGraphOpcodeDescription = EnumToString<PlanGraphOpcode>::Create<
    EnumStringPair<PlanGraphOpcode::UNKNOWN, "UNKNOWN">,
    EnumStringPair<PlanGraphOpcode::VAR, "VAR">,
    EnumStringPair<PlanGraphOpcode::SCAN_NODES, "SCAN_NODES">,
    EnumStringPair<PlanGraphOpcode::FILTER_NODE, "FILTER_NODE">,
    EnumStringPair<PlanGraphOpcode::FILTER_EDGE, "FILTER_EDGE">,
    EnumStringPair<PlanGraphOpcode::GET_OUT_EDGES, "GET_OUT_EDGES">,
    EnumStringPair<PlanGraphOpcode::GET_IN_EDGES, "GET_IN_EDGES">,
    EnumStringPair<PlanGraphOpcode::GET_EDGES, "GET_EDGES">,
    EnumStringPair<PlanGraphOpcode::GET_EDGE_TARGET, "GET_EDGE_TARGET">,
    EnumStringPair<PlanGraphOpcode::CREATE_NODE, "CREATE_NODE">,
    EnumStringPair<PlanGraphOpcode::CREATE_EDGE, "CREATE_EDGE">,
    EnumStringPair<PlanGraphOpcode::CREATE_GRAPH, "CREATE_GRAPH">,
    EnumStringPair<PlanGraphOpcode::PROJECT_RESULTS, "PROJECT_RESULTS">,
    EnumStringPair<PlanGraphOpcode::CARTESIAN_PRODUCT, "CARTESIAN_PRODUCT">,
    EnumStringPair<PlanGraphOpcode::PRODUCE_RESULTS, "PRODUCE_RESULTS">>;


class PlanGraphNode {
public:
    using Nodes = std::vector<PlanGraphNode*>;

    virtual ~PlanGraphNode() = default;

    PlanGraphOpcode getOpcode() const { return _opcode; }

    const Nodes& inputs() const { return _inputs; }

    const Nodes& outputs() const { return _outputs; }

    bool isRoot() const { return _inputs.empty(); }

    const PlanGraphBranch* branch() const { return _branch; }
    PlanGraphBranch* branch() { return _branch; }

    void setBranch(PlanGraphBranch* branch) { _branch = branch; }

    void connectOut(PlanGraphNode* succ) {
        _outputs.emplace_back(succ);
        succ->_inputs.emplace_back(this);
    }

protected:
    explicit PlanGraphNode(PlanGraphOpcode opcode)
        : _opcode(opcode)
    {
    }

private:
    PlanGraphOpcode _opcode {PlanGraphOpcode::UNKNOWN};
    PlanGraphBranch* _branch {nullptr};
    Nodes _inputs;
    Nodes _outputs;
};

}
