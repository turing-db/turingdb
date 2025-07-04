#pragma once

#include <stdint.h>
#include <string>

#include "MatchTarget.h"
#include "metadata/PropertyType.h"

namespace db {

class PropertyTypeMap;
class ASTContext;
class QueryCommand;
class MatchCommand;
class CreateCommand;
class CreateGraphCommand;
class DeclContext;
class EntityPattern;
class LoadGraphCommand;
class ExplainCommand;
class GraphView;
class Expr;
class BinExpr;
class ExprConst;

class QueryAnalyzer {
public:
    QueryAnalyzer(const GraphView& view, ASTContext* ctxt);
    ~QueryAnalyzer();

    bool analyze(QueryCommand* cmd);

private:
    const GraphView& _view;
    ASTContext* _ctxt {nullptr};
    uint64_t _nextNewVarID {0};

    bool analyzeMatch(MatchCommand* cmd);
    bool analyzeCreate(CreateCommand* cmd);
    bool analyzeCreateGraph(CreateGraphCommand* cmd);
    bool analyzeLoadGraph(LoadGraphCommand* cmd);
    bool analyzeEntityPattern(DeclContext* declContext,
                              EntityPattern* entity,
                              bool isCreate);
    void ensureMatchVarsUnique(const MatchTarget* target);
    bool analyzeBinExprConstraint(const BinExpr* constraint, bool isCreate);
    bool typeCheckBinExprConstr(const PropertyType lhs, const ExprConst* rhs);
    bool analyzeExplain(ExplainCommand* cmd);
    std::string createVarName();
};

}
