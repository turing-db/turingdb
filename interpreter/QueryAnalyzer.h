#pragma once

#include "metadata/PropertyType.h"
#include "views/GraphView.h"
#include <stdint.h>
#include <string>

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
class BinExpr;

class QueryAnalyzer {
public:
    QueryAnalyzer(const GraphView& view, ASTContext* ctxt, const PropertyTypeMap& propTypeMap);
    ~QueryAnalyzer();

    bool analyze(QueryCommand* cmd);

private:
    const GraphView& _view;
    ASTContext* _ctxt {nullptr};
    const PropertyTypeMap& _propTypeMap;
    uint64_t _nextNewVarID {0};

    bool analyzeMatch(MatchCommand* cmd);
    bool analyzeCreate(CreateCommand* cmd);
    bool analyzeCreateGraph(CreateGraphCommand* cmd);
    bool analyzeLoadGraph(LoadGraphCommand* cmd);
    bool analyzeEntityPattern(DeclContext* declContext,
                              EntityPattern* entity,
                              bool isCreate);
    bool analyzeBinExprConstraint(const BinExpr* constraint, bool isCreate);
    bool typeCheckBinExprConstr(const ValueType lhs, const ValueType rhs);
    bool analyzeExplain(ExplainCommand* cmd);
    std::string createVarName();
};

}
