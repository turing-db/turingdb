#pragma once

#include <unordered_set>

#include "views/GraphView.h"

namespace db {
class GraphMetadata;
}

namespace db {

class CypherAST;
class PlanGraph;
class PlanGraphNode;
class PlanGraphVariables;
class PlanGraphTopology;
class Stmt;
class MatchStmt;
class CallStmt;
class LoadCSVStmt;
class VectorSearchStmt;
class Skip;
class Limit;
class WhereClause;
class PatternElement;
class Expr;
class VarNode;
class FilterNode;
class NodePattern;
class EdgePattern;
class PropertyExpr;
class EntityTypeExpr;
class VarDecl;
class PlanGenConfig;
class Predicate;

class ReadStmtGenerator {
public:
    ReadStmtGenerator(const CypherAST* ast,
                      GraphView graphView,
                      const PlanGenConfig* config,
                      PlanGraph* tree,
                      PlanGraphVariables* variables);

    ~ReadStmtGenerator();

    ReadStmtGenerator(const ReadStmtGenerator&) = delete;
    ReadStmtGenerator(ReadStmtGenerator&&) = delete;
    ReadStmtGenerator& operator=(const ReadStmtGenerator&) = delete;
    ReadStmtGenerator& operator=(ReadStmtGenerator&&) = delete;

    void generateStmt(const Stmt* stmt);
    void generateMatchStmt(const MatchStmt* stmt);
    void generateCallStmt(const CallStmt* stmt);
    void generateLoadCSVStmt(const LoadCSVStmt* stmt);
    void generateVectorSearchStmt(const VectorSearchStmt* stmt);
    void generateWhereClause(const WhereClause* where);
    void generatePatternElement(const PatternElement* element);

    VarNode* generatePatternElementOrigin(const NodePattern* origin);
    VarNode* generatePatternElementEdge(PlanGraphNode* prevNode, const EdgePattern* edge);
    VarNode* generatePatternElementTarget(PlanGraphNode* prevNode, const NodePattern* target, bool generateGetTarget = true);
    PlanGraphNode* generatePatternElementVariableLengthPath(PlanGraphNode* prevNode, const EdgePattern* edge, const NodePattern* target);

    void unwrapWhereExpr(Expr*);

    void placeJoinsOnVars();
    void placePredicates();
    void placeJoinsOnProcedures();
    PlanGraphNode* generateEndpoint();

    bool insertDataFlowNode(VarNode* node, PlanGraphNode* dependency, Predicate* pred);

    void insertShortestPathNode(VarNode* source,
                                VarNode* target,
                                const PropertyType& edgeType,
                                const VarDecl* distDecl,
                                const VarDecl* pathDecl);

    void setIsStandaloneCall(bool hasReturn) { _isStandaloneCall = hasReturn; }
    void setQueryLimit(size_t limit) { _queryLimit = limit; }

private:
    const CypherAST* _ast {nullptr};
    GraphView _graphView;
    const PlanGenConfig* _config {nullptr};
    const GraphMetadata& _graphMetadata;
    PlanGraph* _tree {nullptr};
    PlanGraphVariables* _variables {nullptr};
    std::unique_ptr<PlanGraphTopology> _topology;
    bool _isStandaloneCall {false};
    size_t _queryLimit {0};
    std::unordered_set<const VarDecl*> _edgesInPattern;

    void generateDependency(PlanGraphNode* producer, Expr* rawExpr);

    bool shouldPlaceValueHashJoin(VarNode* localVar, PlanGraphNode* remoteNode);
    bool tryPlaceValueHashJoin(FilterNode* filter, VarNode* node, PlanGraphNode* dependency, Predicate* pred);
    void placeProcedurePredicate(Predicate* pred);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};
}
