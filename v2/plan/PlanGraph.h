#pragma once

#include <vector>
#include <optional>
#include <ostream>

#include "ID.h"
#include "metadata/LabelSet.h"
#include "EnumToString.h"

namespace db {

class VarDecl;
class BinExpr;
class ExprConstraint;

enum class PlanGraphOpcode {
    UNKNOWN,
    VAR,
    SCAN_NODES,
    SCAN_NODES_BY_LABEL,
    FILTER_NODE_EXPR,
    FILTER_EDGE_EXPR,
    FILTER_NODE_LABEL,
    FILTER_EDGE_TYPE,
    GET_OUT_EDGES,
    GET_EDGE_TARGET,
    CREATE_NODE,
    CREATE_EDGE,
    CREATE_GRAPH,
    _SIZE
};

using PlanGraphOpcodeDescription = EnumToString<PlanGraphOpcode>::Create<
    EnumStringPair<PlanGraphOpcode::UNKNOWN, "UNKNOWN">,
    EnumStringPair<PlanGraphOpcode::VAR, "VAR">,
    EnumStringPair<PlanGraphOpcode::SCAN_NODES, "SCAN_NODES">,
    EnumStringPair<PlanGraphOpcode::SCAN_NODES_BY_LABEL, "SCAN_NODES_BY_LABEL">,
    EnumStringPair<PlanGraphOpcode::FILTER_NODE_EXPR, "FILTER_NODE_EXPR">,
    EnumStringPair<PlanGraphOpcode::FILTER_EDGE_EXPR, "FILTER_EDGE_EXPR">,
    EnumStringPair<PlanGraphOpcode::FILTER_NODE_LABEL, "FILTER_NODE_LABEL">,
    EnumStringPair<PlanGraphOpcode::FILTER_EDGE_TYPE, "FILTER_EDGE_TYPE">,
    EnumStringPair<PlanGraphOpcode::GET_OUT_EDGES, "GET_OUT_EDGES">,
    EnumStringPair<PlanGraphOpcode::GET_EDGE_TARGET, "GET_EDGE_TARGET">,
    EnumStringPair<PlanGraphOpcode::CREATE_NODE, "CREATE_NODE">,
    EnumStringPair<PlanGraphOpcode::CREATE_EDGE, "CREATE_EDGE">,
    EnumStringPair<PlanGraphOpcode::CREATE_GRAPH, "CREATE_GRAPH">>;

class PlanGraphNode {
public:
    using Nodes = std::vector<PlanGraphNode*>;

    explicit PlanGraphNode(PlanGraphOpcode opcode)
        : _opcode(opcode)
    {
    }

    PlanGraphOpcode getOpcode() const { return _opcode; }

    const Nodes& inputs() const { return _inputs; }

    const Nodes& outputs() const { return _outputs; }

    bool isRoot() const { return _inputs.empty(); }

    void connectOut(PlanGraphNode* succ) {
        _outputs.emplace_back(succ);
        succ->_inputs.emplace_back(this);
    }

    VarDecl* getVarDecl() const { return _varDecl; }
    const BinExpr* getExpr() const { return _expr; }
    std::optional<EdgeTypeID> getEdgeTypeID() const { return _edgeTypeID; }
    const LabelSet* getLabelSet() const { return _labelSet; }

    void setVarDecl(VarDecl* varDecl) { _varDecl = varDecl; }
    void setExpr(const BinExpr* expr) { _expr = expr; }
    void setEdgeTypeID(EdgeTypeID edgeTypeID) { _edgeTypeID = edgeTypeID; }
    void setLabelSet(const LabelSet* labelSet) { _labelSet = labelSet; }
    void setExprConstraint(const ExprConstraint* constr) { _exprConstraint = constr; }
    void setString(const std::string& str) { _string = str; }

private:
    PlanGraphOpcode _opcode {PlanGraphOpcode::UNKNOWN};
    Nodes _inputs;
    Nodes _outputs;
    
    // Metadata per node
    VarDecl* _varDecl {nullptr};
    const BinExpr* _expr {nullptr};
    std::optional<EdgeTypeID> _edgeTypeID;
    const LabelSet* _labelSet {nullptr};
    const ExprConstraint* _exprConstraint {nullptr};
    std::string _string;
};

class PlanGraph {
public:
    PlanGraph();
    ~PlanGraph();

    PlanGraphNode* create(PlanGraphOpcode opcode) {
        _nodes.emplace_back(std::make_unique<PlanGraphNode>(opcode));
        return _nodes.back().get();
    }

    void getRoots(std::vector<PlanGraphNode*>& roots) const;

    void dump(std::ostream& out) const ;

private:
    std::vector<std::unique_ptr<PlanGraphNode>> _nodes;
};

}
