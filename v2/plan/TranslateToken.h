#pragma once

#include <stack>

#include "PlanGraphStream.h"

namespace db::v2 {

class PlanGraphNode;

struct TranslateToken {
    PlanGraphNode* node;
    PlanGraphStream stream;
};

using TranslateTokenStack = std::stack<TranslateToken>;

}
