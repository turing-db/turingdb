#pragma once

#include <array>

namespace db {

class VarDecl;

enum class PlanTreeOpcode {
    UNKNOWN,
    VAR,
    SCAN_NODES,
    SCAN_NODES_BY_LABEL,
    FILTER
};

class VarDecl;

class PlanTreeNode {
public:
    PlanTreeNode()
    {
        init();
    }

    PlanTreeNode(PlanTreeOpcode opcode)
        : _opcode(opcode)
    {
        init();
    }

    PlanTreeOpcode getOpcode() const { return _opcode; }

    void setInput(PlanTreeNode* input) { _input = input; }

    void setOutput(std::size_t childNum, PlanTreeNode* child) {
        _children[childNum] = child;
    }

    void setVarDecl(VarDecl* varDecl) { _varDecl = varDecl; }
    VarDecl* getVarDecl() const { return _varDecl; }

private:
    static constexpr std::size_t MAX_CHILDREN = 4;

    PlanTreeOpcode _opcode {PlanTreeOpcode::UNKNOWN};
    PlanTreeNode* _input {nullptr};
    std::array<PlanTreeNode*, MAX_CHILDREN> _children;
    
    //Metadata
    VarDecl* _varDecl {nullptr};

    void init() {
        _children.fill(nullptr);
    }
};

}
