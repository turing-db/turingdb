#pragma once

#include <string_view>

#include "PlanGraph.h"

namespace db {
class GraphView;
}

namespace db::v2 {

class CypherAST;
class PlanGraph;
class PlanGraphVariables;
class SinglePartQuery;
class ChangeQuery;
class ReturnStmt;
class LoadGraphQuery;
class ListGraphQuery;
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
    std::unique_ptr<PlanGraphVariables> _variables;

    void generateChangeQuery(const ChangeQuery* query);
    void generateSinglePartQuery(const SinglePartQuery* query);
    void generateReturnStmt(const ReturnStmt* stmt, PlanGraphNode* prevNode);
    void generateLoadGraphQuery(const LoadGraphQuery* query);
    void generateListGraphQuery(const ListGraphQuery* query);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};
}
