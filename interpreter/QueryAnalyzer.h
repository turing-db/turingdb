#pragma once

#include <stdint.h>
#include <string>

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
class BinExpr;

class QueryAnalyzer {
public:
    QueryAnalyzer(const GraphView& view,
                  ASTContext* ctxt,
                  const PropertyTypeMap& propTypeMap);
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
    bool typeCheckBinExprConstr(ValueType lhs, ValueType rhs);
    bool analyzeExplain(ExplainCommand* cmd);
    std::string createVarName();
};

}
