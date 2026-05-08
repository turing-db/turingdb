#include "JoinNode.h"
#include "decl/EvaluatedType.h"

using namespace db;

// NOTE: This should be synced with @ref ValueHashJoinPairs
bool JoinNode::joinableTypes(EvaluatedType a, EvaluatedType b) {
    const bool same = a == b;
    return same;
}
