#pragma once

#include <stdint.h>
#include <string>

#include "MatchTarget.h"
#include "metadata/PropertyType.h"

namespace db {

class PropertyTypeMap;
class GraphView;
class ASTContext;
class QueryCommand;
class MatchCommand;
class CreateCommand;
class CreateGraphCommand;
class DeclContext;
class EntityPattern;
class LoadGraphCommand;
class ExplainCommand;
class S3TransferCommand;
class GraphView;
class Expr;
class BinExpr;
class ExprConst;

class QueryAnalyzer {
public:
    QueryAnalyzer(const GraphView& view, ASTContext* ctxt);
    ~QueryAnalyzer();

    void analyze(QueryCommand* cmd);

private:
    const GraphView& _view;
    ASTContext* _ctxt {nullptr};
    uint64_t _nextNewVarID {0};

    void analyzeMatch(MatchCommand* cmd);
    void analyzeCreate(CreateCommand* cmd);
    void analyzeCreateGraph(CreateGraphCommand* cmd);
    void analyzeLoadGraph(LoadGraphCommand* cmd);
    void analyzeEntityPattern(DeclContext* declContext,
                              EntityPattern* entity,
                              bool isCreate);
    void ensureValidTypeConstraints(EntityPattern* entity);
    void ensureMatchVarsUnique(const MatchTarget* target);
    void analyzeBinExprConstraint(const BinExpr* constraint, bool isCreate);
    void typeCheckBinExprConstr(const PropertyType lhs, const ExprConst* rhs);
    void analyzeExplain(ExplainCommand* cmd);
    void analyzeS3Transfer(S3TransferCommand* cmd);
    std::string createVarName();
};
}
