#pragma once

#include <ostream>
#include <unordered_set>

namespace db::v2 {

class CypherAST;
class SinglePartQuery;
class ChangeQuery;
class CommitQuery;
class MatchStmt;
class Limit;
class Skip;
class ReturnStmt;
class Projection;
class Pattern;
class PatternElement;
class WhereClause;
class Expr;
class NodePattern;
class EdgePattern;
class MapLiteral;
class BinaryExpr;
class UnaryExpr;
class SymbolExpr;
class LiteralExpr;
class ParameterExpr;
class PathExpr;
class EntityTypeExpr;
class StringExpr;
class PropertyExpr;
class VarDecl;
class FunctionInvocationExpr;
class LoadGraphQuery;
class ListGraphQuery;
class CreateGraphQuery;
class LoadGMLQuery;
class LoadNeo4jQuery;
class S3ConnectQuery;
class S3TransferQuery;

class NodePatternData;
class EdgePatternData;

class CypherASTDumper {
public:
    explicit CypherASTDumper(const CypherAST* ast);
    ~CypherASTDumper() = default;

    CypherASTDumper(const CypherASTDumper&) = delete;
    CypherASTDumper& operator=(const CypherASTDumper&) = delete;
    CypherASTDumper(CypherASTDumper&&) = delete;
    CypherASTDumper& operator=(CypherASTDumper&&) = delete;

    void dump(std::ostream& output);

private:
    const CypherAST* _ast {nullptr};
    std::unordered_set<const VarDecl*> _dumpedVariables;

    void dump(std::ostream& out, const SinglePartQuery* query);
    void dump(std::ostream& out, const LoadGraphQuery* query);
    void dump(std::ostream& out, const LoadNeo4jQuery* query);
    void dump(std::ostream& out, const ChangeQuery* query);
    void dump(std::ostream& out, const CommitQuery* query);
    void dump(std::ostream& out, const ListGraphQuery* query);
    void dump(std::ostream& out, const CreateGraphQuery* query);
    void dump(std::ostream& out, const LoadGMLQuery* query);
    void dump(std::ostream& out, const S3ConnectQuery* query);
    void dump(std::ostream& out, const S3TransferQuery* query);
    void dump(std::ostream& out, const MatchStmt* match);
    void dump(std::ostream& out, const Limit* lim);
    void dump(std::ostream& out, const Skip* skip);
    void dump(std::ostream& out, const ReturnStmt* ret);
    void dump(std::ostream& out, const Projection* projection);
    void dump(std::ostream& out, const Pattern* pattern);
    void dump(std::ostream& out, const PatternElement* elem);
    void dump(std::ostream& out, const WhereClause* where);
    void dump(std::ostream& out, const NodePattern* node);
    void dump(std::ostream& out, const EdgePattern* edge);
    void dump(std::ostream& out, const MapLiteral* map);
    void dump(std::ostream& out, const Expr* expr);
    void dump(std::ostream& out, const BinaryExpr* expr);
    void dump(std::ostream& out, const UnaryExpr* expr);
    void dump(std::ostream& out, const SymbolExpr* expr);
    void dump(std::ostream& out, const LiteralExpr* expr);
    void dump(std::ostream& out, const PathExpr* expr);
    void dump(std::ostream& out, const EntityTypeExpr* expr);
    void dump(std::ostream& out, const StringExpr* expr);
    void dump(std::ostream& out, const PropertyExpr* expr);
    void dump(std::ostream& out, const VarDecl* decl);
    void dump(std::ostream& out, const FunctionInvocationExpr* expr);
};

}

