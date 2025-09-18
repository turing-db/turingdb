#pragma once

#include <vector>
#include <unordered_map>
#include <stdint.h>
#include <string_view>

#include "SourceLocation.h"

namespace db::v2 {

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

    std::string_view getQueryString() const { return _queryStr; }

    const QueryCommands& queries() const { return _queries; }

    void setDebugLocations(bool debugLocations) { _debugLocations = debugLocations; }

    template <typename T>
    const SourceLocation* getLocation(T* obj) const {
        return getLocation((uintptr_t)obj);
    }

private:
    std::string_view _queryStr;
    bool _debugLocations {true};
    std::vector<Symbol*> _symbols;
    std::vector<QualifiedName*> _qualifiedNames;
    std::vector<Literal*> _literals;
    std::vector<Expr*> _expressions;
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

    // Locations
    std::unordered_map<uintptr_t, SourceLocation> _locations;

    template <typename T>
    void setLocation(T* obj, const SourceLocation& loc) {
        setLocation((uintptr_t)obj, loc);
    }

    void setLocation(uintptr_t obj, const SourceLocation& loc);
    const SourceLocation* getLocation(uintptr_t obj) const;

    void addSymbol(Symbol* symbol);
    void addQualifiedName(QualifiedName* name);
    void addLiteral(Literal* literal);
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
