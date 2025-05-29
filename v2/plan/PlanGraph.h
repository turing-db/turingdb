#pragma once

#include <vector>
#include <optional>

#include "EntityID.h"
#include "metadata/LabelSet.h"
#include "Arena.h"

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
    CREATE_GRAPH
};

class PlanGraphNode {
public:
    PlanGraphNode()
    {
    }

    PlanGraphNode(PlanGraphOpcode opcode)
        : _opcode(opcode)
    {
    }

    PlanGraphOpcode getOpcode() const { return _opcode; }

    VarDecl* getVarDecl() const { return _varDecl; }
    const BinExpr* getExpr() const { return _expr; }
    std::optional<EdgeTypeID> getEdgeTypeID() const { return _edgeTypeID; }
    const LabelSet* getLabelSet() const { return _labelSet; }

    void connectOut(PlanGraphNode* succ) {
        _outputs.emplace_back(succ);
        succ->_inputs.emplace_back(this);
    }

    void setVarDecl(VarDecl* varDecl) { _varDecl = varDecl; }
    void setExpr(const BinExpr* expr) { _expr = expr; }
    void setEdgeTypeID(EdgeTypeID edgeTypeID) { _edgeTypeID = edgeTypeID; }
    void setLabelSet(const LabelSet* labelSet) { _labelSet = labelSet; }
    void setExprConstraint(const ExprConstraint* exprConstraint) { _exprConstraint = exprConstraint; }
    void setString(const std::string& str) { _string = str; }

private:
    static constexpr std::size_t MAX_OUTS = 4;

    PlanGraphOpcode _opcode {PlanGraphOpcode::UNKNOWN};
    std::vector<PlanGraphNode*> _inputs;
    std::vector<PlanGraphNode*> _outputs;
    
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
        return &_nodes.emplace_back(opcode);
    }

private:
    Arena<PlanGraphNode> _nodes;
};

}
