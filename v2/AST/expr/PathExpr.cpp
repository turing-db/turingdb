#include "PathExpr.h"

#include "CypherAST.h"
#include "PatternElement.h"
#include "NodePattern.h"
#include "EdgePattern.h"

using namespace db::v2;

PathExpr::~PathExpr() {
}

void PathExpr::addNode(NodePattern* node) {
    _pattern->addNode(node);
}

void PathExpr::addEdge(EdgePattern* edge) {
    _pattern->addEdge(edge);
}

PathExpr* PathExpr::create(CypherAST* ast, PatternElement* pattern) {
    PathExpr* expr = new PathExpr(pattern);
    ast->addExpr(expr);
    return expr;
}
