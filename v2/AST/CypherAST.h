#pragma once

#include <memory>
#include <vector>
#include <stdint.h>
#include <string_view>

namespace db::v2 {

class SourceManager;
class DiagnosticsManager;
class Symbol;
class SymbolChain;
class QualifiedName;
class FunctionInvocation;
class Literal;
class NullLiteral;
class BoolLiteral;
class IntegerLiteral;
class DoubleLiteral;
class StringLiteral;
class CharLiteral;
class MapLiteral;
class WildcardLiteral;
class Expr;
class ExprChain;
class BinaryExpr;
class LiteralExpr;
class EntityTypeExpr;
class ParameterExpr;
class PathExpr;
class PropertyExpr;
class StringExpr;
class SymbolExpr;
class UnaryExpr;
class FunctionInvocationExpr;
class Pattern;
class PatternElement;
class EntityPattern;
class NodePattern;
class EdgePattern;
class Projection;
class Stmt;
class SubStmt;
class SetItem;
class Limit;
class Skip;
class OrderBy;
class OrderByItem;
class StmtContainer;
class MatchStmt;
class CallStmt;
class CreateStmt;
class SetStmt;
class DeleteStmt;
class QueryCommand;
class SinglePartQuery;
class ChangeQuery;
class ReturnStmt;
class WhereClause;
class YieldClause;
class YieldItems;
class DeclContext;
class VarDecl;
class NodePatternData;
class EdgePatternData;
class CypherParserState;
class YCypherParser;
class ListGraphQuery;
class FunctionDecls;
class ProcedureBlueprintMap;
class LoadGraphQuery;

class CypherAST {
public:
    friend Symbol;
    friend SymbolChain;
    friend QualifiedName;
    friend FunctionInvocation;
    friend Literal;
    friend NullLiteral;
    friend BoolLiteral;
    friend IntegerLiteral;
    friend DoubleLiteral;
    friend StringLiteral;
    friend CharLiteral;
    friend MapLiteral;
    friend WildcardLiteral;
    friend ExprChain;
    friend BinaryExpr;
    friend LiteralExpr;
    friend EntityTypeExpr;
    friend ParameterExpr;
    friend PathExpr;
    friend PropertyExpr;
    friend StringExpr;
    friend SymbolExpr;
    friend UnaryExpr;
    friend FunctionInvocationExpr;
    friend Pattern;
    friend PatternElement;
    friend NodePattern;
    friend EdgePattern;
    friend Projection;
    friend SetItem;
    friend Limit;
    friend Skip;
    friend OrderBy;
    friend OrderByItem;
    friend StmtContainer;
    friend MatchStmt;
    friend CallStmt;
    friend CreateStmt;
    friend SetStmt;
    friend DeleteStmt;
    friend ReturnStmt;
    friend WhereClause;
    friend YieldClause;
    friend YieldItems;
    friend SinglePartQuery;
    friend ChangeQuery;
    friend DeclContext;
    friend VarDecl;
    friend NodePatternData;
    friend EdgePatternData;
    friend LoadGraphQuery;
    friend ListGraphQuery;
    friend YCypherParser;

    using QueryCommands = std::vector<QueryCommand*>;

    CypherAST(const ProcedureBlueprintMap& procedures,
              std::string_view queryString);
    ~CypherAST();

    CypherAST(const CypherAST&) = delete;
    CypherAST(CypherAST&&) = delete;
    CypherAST& operator=(const CypherAST&) = delete;
    CypherAST& operator=(CypherAST&&) = delete;

    const QueryCommands& queries() const { return _queries; }

    FunctionDecls& functionDecls() { return *_functionDecls; }
    const FunctionDecls& functionDecls() const { return *_functionDecls; }

    std::string* createString();

    SourceManager* getSourceManager() { return _sourceManager; }
    const SourceManager* getSourceManager() const { return _sourceManager; }

    DiagnosticsManager* getDiagnosticsManager() { return _diagnosticsManager; }
    const DiagnosticsManager* getDiagnosticsManager() const { return _diagnosticsManager; }

    FunctionDecls* getFunctionDecls() { return _functionDecls.get(); }
    const FunctionDecls* getFunctionDecls() const { return _functionDecls.get(); }

private:
    SourceManager* _sourceManager {nullptr};
    DiagnosticsManager* _diagnosticsManager {nullptr};

    std::vector<Symbol*> _symbols;
    std::vector<SymbolChain*> _symbolChains;
    std::vector<QualifiedName*> _qualifiedNames;
    std::vector<FunctionInvocation*> _functionInvocations;
    std::vector<Literal*> _literals;
    std::vector<Expr*> _expressions;
    std::vector<ExprChain*> _exprChains;
    std::vector<Pattern*> _patterns;
    std::vector<PatternElement*> _patternElems;
    std::vector<EntityPattern*> _entityPatterns;
    std::vector<Projection*> _projections;
    std::vector<WhereClause*> _whereClauses;
    std::vector<YieldClause*> _yieldClauses;
    std::vector<YieldItems*> _yieldItems;
    std::vector<Stmt*> _stmts;
    std::vector<SubStmt*> _subStmts;
    std::vector<SetItem*> _setItems;
    std::vector<OrderBy*> _orderBys;
    std::vector<OrderByItem*> _orderByItems;
    std::vector<StmtContainer*> _stmtContainers;
    QueryCommands _queries;
    std::vector<DeclContext*> _declContexts;
    std::vector<VarDecl*> _varDecls;
    std::vector<NodePatternData*> _nodePatternDatas;
    std::vector<EdgePatternData*> _edgePatternDatas;
    std::vector<std::string*> _unnamedVarIdentifiers;

    std::unique_ptr<FunctionDecls> _functionDecls;

    void addSymbol(Symbol* symbol);
    void addSymbolChain(SymbolChain* symbol);
    void addQualifiedName(QualifiedName* name);
    void addLiteral(Literal* literal);
    void addExprChain(ExprChain* exprChain);
    void addExpr(Expr* expr);
    void addPattern(Pattern* pattern);
    void addPatternElement(PatternElement* element);
    void addEntityPattern(EntityPattern* pattern);
    void addProjection(Projection* projection);
    void addWhereClause(WhereClause* clause);
    void addYieldClause(YieldClause* clause);
    void addYieldItems(YieldItems* items);
    void addStmt(Stmt* stmt);
    void addSubStmt(SubStmt* subStmt);
    void addSetItem(SetItem* item);
    void addOrderByItem(OrderByItem* item);
    void addStmtContainer(StmtContainer* container);
    void addQuery(QueryCommand* query);
    void addDeclContext(DeclContext* ctxt);
    void addVarDecl(VarDecl* decl);
    void addNodePatternData(NodePatternData* data);
    void addEdgePatternData(EdgePatternData* data);
    void addFunctionInvocation(FunctionInvocation* invocation);
};

}
