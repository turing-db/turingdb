#pragma once

#include "decl/VarDecl.h"
#include "nodes/PlanGraphNode.h"

namespace db {

enum class JoinType : uint8_t {
    COMMON_ANCESTOR = 0,
    COMMON_SUCCESSOR,
    DIAMOND, // Common Ancestor + Common Sucessor
    PREDICATE,
};

class JoinNode : public PlanGraphNode {
public:
    JoinNode() = delete;
    JoinNode(const VarDecl* leftJoinKeyVar,
             const VarDecl* rightJoinKeyVar,
             JoinType type)
        : PlanGraphNode(PlanGraphOpcode::JOIN),
        _leftJoinKeyVar(leftJoinKeyVar),
        _rightJoinKeyVar(rightJoinKeyVar),
        _type(type)
    {
    }

    const VarDecl* getLeftVarDecl() const { return _leftJoinKeyVar; };
    const VarDecl* getRightVarDecl() const { return _rightJoinKeyVar; };
    JoinType getJoinType() const { return _type; };

private:
    const VarDecl* _leftJoinKeyVar {nullptr};
    const VarDecl* _rightJoinKeyVar {nullptr};

    JoinType _type;
};

}
