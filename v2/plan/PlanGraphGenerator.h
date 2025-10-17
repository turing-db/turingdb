#pragma once

#include <string_view>

#include "PlanGraphVariables.h"

#include "PlanGraph.h"

namespace db {
class GraphView;
}

namespace db::v2 {

class CypherAST;
class PlanGraph;
class PropertyConstraint;
class SinglePartQuery;
class ReturnStmt;
class QueryCommand;

class PlanGraphGenerator {
public:
    PlanGraphGenerator(const CypherAST& ast,
                       const GraphView& view);
    ~PlanGraphGenerator();

    PlanGraph& getPlanGraph() { return _tree; }

    void generate(const QueryCommand* query);

private:
    const CypherAST* _ast {nullptr};
    const GraphView& _view;
    PlanGraph _tree;
    PlanGraphVariables _variables;

    void generateSinglePartQuery(const SinglePartQuery* query);
    void generateReturnStmt(const ReturnStmt* stmt, PlanGraphNode* prevNode);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
