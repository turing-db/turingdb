#include "ExprEvalNode.h"

using namespace db;

ExprEvalNode::ExprEvalNode()
    : PlanGraphNode(PlanGraphOpcode::EXPR_EVAL)
{
}

ExprEvalNode::~ExprEvalNode()
{
}
