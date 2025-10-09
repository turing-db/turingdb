#pragma once

#include <vector>
#include <stdint.h>
#include <string_view>

namespace db::v2 {

class SourceManager;
class DiagnosticsManager;
class Symbol;
class QualifiedName;
class Literal;
class NullLiteral;
class BoolLiteral;
class IntegerLiteral;
class DoubleLiteral;
class StringLiteral;
class CharLiteral;
class MapLiteral;
class Expr;
class ExprChain;
class BinaryExpr;
class LiteralExpr;
class NodeLabelExpr;
class ParameterExpr;
class PathExpr;
class PropertyExpr;
class StringExpr;
class SymbolExpr;
class UnaryExpr;
class Pattern;
class PatternElement;
class EntityPattern;
class NodePattern;
class EdgePattern;
class Projection;
class Stmt;
class SubStmt;
class Limit;
class Skip;
class StmtContainer;
class MatchStmt;
class CreateStmt;
class DeleteStmt;
class QueryCommand;
class SinglePartQuery;
class ReturnStmt;
class WhereClause;
class DeclContext;
class VarDecl;
class NodePatternData;
class EdgePatternData;
class CypherParserState;
class YCypherParser;

class CypherAST {
public:
    friend Symbol;
    friend QualifiedName;
    friend Literal;
    friend NullLiteral;
    friend BoolLiteral;
    friend IntegerLiteral;
    friend DoubleLiteral;
    friend StringLiteral;
    friend CharLiteral;
    friend MapLiteral;
    friend ExprChain;
    friend BinaryExpr;
    friend LiteralExpr;
    friend NodeLabelExpr;
    friend ParameterExpr;
    friend PathExpr;
    friend PropertyExpr;
    friend StringExpr;
    friend SymbolExpr;
    friend UnaryExpr;
    friend Pattern;
    friend PatternElement;
    friend NodePattern;
    friend EdgePattern;
    friend Projection;
    friend Limit;
    friend Skip;
    friend StmtContainer;
    friend MatchStmt;
    friend CreateStmt;
    friend DeleteStmt;
    friend ReturnStmt;
    friend WhereClause;
    friend SinglePartQuery;
    friend DeclContext;
    friend VarDecl;
    friend NodePatternData;
    friend EdgePatternData;
    friend YCypherParser;

    using QueryCommands = std::vector<QueryCommand*>;

    CypherAST(std::string_view queryString);
    ~CypherAST();

    CypherAST(const CypherAST&) = delete;
    CypherAST(CypherAST&&) = delete;
    CypherAST& operator=(const CypherAST&) = delete;
    CypherAST& operator=(CypherAST&&) = delete;

    const QueryCommands& queries() const { return _queries; }

    std::string* createString();

    SourceManager* getSourceManager() { return _sourceManager; }
    const SourceManager* getSourceManager() const { return _sourceManager; }

    DiagnosticsManager* getDiagnosticsManager() { return _diagnosticsManager; }
    const DiagnosticsManager* getDiagnosticsManager() const { return _diagnosticsManager; }

private:
    SourceManager* _sourceManager {nullptr};
    DiagnosticsManager* _diagnosticsManager {nullptr};

    std::vector<Symbol*> _symbols;
    std::vector<QualifiedName*> _qualifiedNames;
    std::vector<Literal*> _literals;
    std::vector<Expr*> _expressions;
    std::vector<ExprChain*> _exprChains;
    std::vector<Pattern*> _patterns;
    std::vector<PatternElement*> _patternElems;
    std::vector<EntityPattern*> _entityPatterns;
    std::vector<Projection*> _projections;
    std::vector<WhereClause*> _whereClauses;
    std::vector<Stmt*> _stmts;
    std::vector<SubStmt*> _subStmts;
    std::vector<StmtContainer*> _stmtContainers;
    QueryCommands _queries;
    std::vector<DeclContext*> _declContexts;
    std::vector<VarDecl*> _varDecls;
    std::vector<NodePatternData*> _nodePatternDatas;
    std::vector<EdgePatternData*> _edgePatternDatas;
    std::vector<std::string*> _unnamedVarIdentifiers;

    void addSymbol(Symbol* symbol);
    void addQualifiedName(QualifiedName* name);
    void addLiteral(Literal* literal);
    void addExprChain(ExprChain* exprChain);
    void addExpr(Expr* expr);
    void addPattern(Pattern* pattern);
    void addPatternElement(PatternElement* element);
    void addEntityPattern(EntityPattern* pattern);
    void addProjection(Projection* projection);
    void addWhereClause(WhereClause* clause);
    void addStmt(Stmt* stmt);
    void addSubStmt(SubStmt* subStmt);
    void addStmtContainer(StmtContainer* container);
    void addQuery(QueryCommand* query);
    void addDeclContext(DeclContext* ctxt);
    void addVarDecl(VarDecl* decl);
    void addNodePatternData(NodePatternData* data);
    void addEdgePatternData(EdgePatternData* data);
};

}
