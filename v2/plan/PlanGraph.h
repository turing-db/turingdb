#pragma once

#include <vector>
#include <ostream>

#include "ID.h"
#include "metadata/LabelSet.h"
#include "EnumToString.h"

namespace db::v2 {

class VarDecl;
class Expr;
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
    GET_IN_EDGES,
    GET_EDGES,
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
    EnumStringPair<PlanGraphOpcode::GET_IN_EDGES, "GET_IN_EDGES">,
    EnumStringPair<PlanGraphOpcode::GET_EDGES, "GET_EDGES">,
    EnumStringPair<PlanGraphOpcode::GET_EDGE_TARGET, "GET_EDGE_TARGET">,
    EnumStringPair<PlanGraphOpcode::CREATE_NODE, "CREATE_NODE">,
    EnumStringPair<PlanGraphOpcode::CREATE_EDGE, "CREATE_EDGE">,
    EnumStringPair<PlanGraphOpcode::CREATE_GRAPH, "CREATE_GRAPH">>;

class PlanGraphNode {
public:
    using Nodes = std::vector<PlanGraphNode*>;

    virtual ~PlanGraphNode() = default;

    PlanGraphOpcode getOpcode() const { return _opcode; }

    const Nodes& inputs() const { return _inputs; }

    const Nodes& outputs() const { return _outputs; }

    bool isRoot() const { return _inputs.empty(); }

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
    Nodes _inputs;
    Nodes _outputs;
};

class PlanGraph {
public:
    PlanGraph();
    ~PlanGraph();

    template<typename T, typename... Args>
    T* create(Args&&... args) {
        auto node = std::make_unique<T>(std::forward<Args>(args)...);
        auto* nodePtr = node.get();
        _nodes.emplace_back(std::move(node));
        return nodePtr;
    }

    void getRoots(std::vector<PlanGraphNode*>& roots) const;

    void dump(std::ostream& out) const ;

private:
    friend class PlanGraphDebug;

    std::vector<std::unique_ptr<PlanGraphNode>> _nodes;
};

class VarNode : public PlanGraphNode {
public:
    explicit VarNode(const VarDecl* varDecl)
        : PlanGraphNode(PlanGraphOpcode::VAR),
        _varDecl(varDecl)
    {
    }

    const VarDecl* getVarDecl() const { return _varDecl; }

private:
    const VarDecl* _varDecl {nullptr};
};

class ScanNodesNode : public PlanGraphNode {
public:
    explicit ScanNodesNode() 
        : PlanGraphNode(PlanGraphOpcode::SCAN_NODES)
    {}
};

class ScanNodesByLabelNode : public PlanGraphNode {
public:
    explicit ScanNodesByLabelNode(const LabelSet* labelSet) 
        : PlanGraphNode(PlanGraphOpcode::SCAN_NODES_BY_LABEL),
        _labelSet(labelSet)
    {
    }

    const LabelSet* getLabelSet() const { return _labelSet; }

private:
    const LabelSet* _labelSet {nullptr};
};

class FilterNodeExprNode : public PlanGraphNode {
public:
    explicit FilterNodeExprNode(const Expr* expr) 
        : PlanGraphNode(PlanGraphOpcode::FILTER_NODE_EXPR),
        _expr(expr)
    {
    }

    const Expr* getExpr() const { return _expr; }

private:
    const Expr* _expr {nullptr};
};

class FilterEdgeExprNode : public PlanGraphNode {
public:
    explicit FilterEdgeExprNode(const Expr* expr) 
        : PlanGraphNode(PlanGraphOpcode::FILTER_EDGE_EXPR),
        _expr(expr)
    {
    }

    const Expr* getExpr() const { return _expr; }

private:
    const Expr* _expr {nullptr};
};

class FilterNodeLabelNode : public PlanGraphNode {
public:
    explicit FilterNodeLabelNode(const LabelSet* labelSet) 
        : PlanGraphNode(PlanGraphOpcode::FILTER_NODE_LABEL),
        _labelSet(labelSet)
    {
    }

    const LabelSet* getLabelSet() const { return _labelSet; }

private:
    const LabelSet* _labelSet {nullptr};
};

class FilterEdgeTypeNode : public PlanGraphNode {
public:
    explicit FilterEdgeTypeNode(EdgeTypeID edgeTypeID) 
        : PlanGraphNode(PlanGraphOpcode::FILTER_EDGE_TYPE),
        _edgeTypeID(edgeTypeID)
    {
    }

    EdgeTypeID getEdgeTypeID() const { return _edgeTypeID; }

private:
    EdgeTypeID _edgeTypeID;
};

class GetOutEdgesNode : public PlanGraphNode {
public:
    explicit GetOutEdgesNode()
        : PlanGraphNode(PlanGraphOpcode::GET_OUT_EDGES)
    {
    }
};

class GetInEdgesNode : public PlanGraphNode {
public:
    explicit GetInEdgesNode()
        : PlanGraphNode(PlanGraphOpcode::GET_IN_EDGES)
    {
    }
};

class GetEdgesNode : public PlanGraphNode {
public:
    explicit GetEdgesNode()
        : PlanGraphNode(PlanGraphOpcode::GET_EDGES)
    {
    }
};

class GetEdgeTargetNode : public PlanGraphNode {
public:
    explicit GetEdgeTargetNode()
        : PlanGraphNode(PlanGraphOpcode::GET_EDGE_TARGET)
    {
    }

};

class CreateNodeNode : public PlanGraphNode {
public:
    explicit CreateNodeNode()
        : PlanGraphNode(PlanGraphOpcode::CREATE_NODE)
    {
    }

    void setLabelSet(const LabelSet* labelSet) { _labelSet = labelSet; }
    void setExprConstraint(const ExprConstraint* exprConstraint) { _exprConstraint = exprConstraint; }

    const LabelSet* getLabelSet() const { return _labelSet; }

    const ExprConstraint* getExprConstraint() const { return _exprConstraint; }

private:
    const LabelSet* _labelSet {nullptr};
    const ExprConstraint* _exprConstraint {nullptr};
};

class CreateEdgeNode : public PlanGraphNode {
public:
    explicit CreateEdgeNode()
        : PlanGraphNode(PlanGraphOpcode::CREATE_EDGE)
    {
    }
};

class CreateGraphNode : public PlanGraphNode {
public:
    explicit CreateGraphNode(const std::string& graphName)
        : PlanGraphNode(PlanGraphOpcode::CREATE_GRAPH),
        _graphName(graphName)
    {
    }

    const std::string& getGraphName() const { return _graphName; }

private:
    std::string _graphName;
};

}
