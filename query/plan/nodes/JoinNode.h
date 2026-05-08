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
    JoinNode(PlanGraphNodeID id,
             const VarDecl* joinKeyVar1,
             const VarDecl* joinKeyVar2,
             JoinType type)
        : PlanGraphNode(id, PlanGraphOpcode::JOIN),
        _fstJoinKeyVar(joinKeyVar1),
        _sndJoinKeyVar(joinKeyVar2),
        _type(type)
    {
    }

    JoinNode(PlanGraphNodeID id,
             const VarDecl* joinKeyVar1,
             const VarDecl* joinKeyVar2,
             const VarDecl* dependencyDecl,
             JoinType type)
        : PlanGraphNode(id, PlanGraphOpcode::JOIN),
        _fstJoinKeyVar(joinKeyVar1),
        _sndJoinKeyVar(joinKeyVar2),
        _dependencyVarDecl(dependencyDecl),
        _type(type)
    {
    }

    const VarDecl* getFirstJoinKey() const { return _fstJoinKeyVar; }
    const VarDecl* getSecondJoinKey() const { return _sndJoinKeyVar; }
    const VarDecl* getDependencyVarDecl() const { return _dependencyVarDecl; }
    JoinType getJoinType() const { return _type; };

    static bool joinableTypes(EvaluatedType a, EvaluatedType b);

private:
    const VarDecl* _fstJoinKeyVar {nullptr};
    const VarDecl* _sndJoinKeyVar {nullptr};

    const VarDecl* _dependencyVarDecl {nullptr};

    JoinType _type {JoinType::COMMON_ANCESTOR};
};
}
