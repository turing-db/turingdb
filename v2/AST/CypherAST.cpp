#include "CypherAST.h"

#include "Symbol.h"
#include "QualifiedName.h"
#include "expr/Literal.h"
#include "expr/Expr.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "EntityPattern.h"
#include "Projection.h"
#include "WhereClause.h"
#include "stmt/Stmt.h"
#include "stmt/SubStmt.h"
#include "stmt/StmtContainer.h"
#include "QueryCommand.h"
#include "decl/DeclContext.h"
#include "decl/VarDecl.h"
#include "decl/PatternData.h"

using namespace db::v2;

CypherAST::CypherAST(std::string_view queryString)
    : _queryStr(queryString)
{
}

CypherAST::~CypherAST() {
    for (Symbol* symbol : _symbols) {
        delete symbol;
    }

    for (QualifiedName* name : _qualifiedNames) {
        delete name;
    }

    for (Literal* literal : _literals) {
        delete literal;
    }

    for (Expr* expr : _expressions) {
        delete expr;
    }

    for (Pattern* pattern : _patterns) {
        delete pattern;
    }

    for (PatternElement* element : _patternElems) {
        delete element;
    }

    for (EntityPattern* pattern : _entityPatterns) {
        delete pattern;
    }

    for (Projection* projection : _projections) {
        delete projection;
    }

    for (WhereClause* clause : _whereClauses) {
        delete clause;
    }

    for (Stmt* stmt : _stmts) {
        delete stmt;
    }

    for (SubStmt* subStmt : _subStmts) {
        delete subStmt;
    }

    for (StmtContainer* container : _stmtContainers) {
        delete container;
    }

    for (QueryCommand* query : _queries) {
        delete query;
    }

    for (DeclContext* ctxt : _declContexts) {
        delete ctxt;
    }

    for (VarDecl* decl : _varDecls) {
        delete decl;
    }

    for (NodePatternData* data : _nodePatternDatas) {
        delete data;
    }

    for (EdgePatternData* data : _edgePatternDatas) {
        delete data;
    }
}

void CypherAST::setLocation(uintptr_t obj, const SourceLocation& loc) {
    _locations[obj] = loc;
}

const SourceLocation* CypherAST::getLocation(uintptr_t obj) const {
    const auto it = _locations.find(obj);
    if (it == _locations.end()) {
        return nullptr;
    }

    return &it->second;
}

void CypherAST::addSymbol(Symbol* symbol) {
    _symbols.push_back(symbol);
}

void CypherAST::addQualifiedName(QualifiedName* name) {
    _qualifiedNames.push_back(name);
}

void CypherAST::addLiteral(Literal* literal) {
    _literals.push_back(literal);
}

void CypherAST::addExpr(Expr* expr) {
    _expressions.push_back(expr);
}

void CypherAST::addPattern(Pattern* pattern) {
    _patterns.push_back(pattern);
}

void CypherAST::addPatternElement(PatternElement* element) {
    _patternElems.push_back(element);
}

void CypherAST::addEntityPattern(EntityPattern* pattern) {
    _entityPatterns.push_back(pattern);
}

void CypherAST::addProjection(Projection* projection) {
    _projections.push_back(projection);
}

void CypherAST::addWhereClause(WhereClause* clause) {
    _whereClauses.push_back(clause);
}

void CypherAST::addStmt(Stmt* stmt) {
    _stmts.push_back(stmt);
}

void CypherAST::addSubStmt(SubStmt* subStmt) {
    _subStmts.push_back(subStmt);
}

void CypherAST::addStmtContainer(StmtContainer* container) {
    _stmtContainers.push_back(container);
}

void CypherAST::addQuery(QueryCommand* query) {
    _queries.push_back(query);
}

void CypherAST::addDeclContext(DeclContext* ctxt) {
    _declContexts.push_back(ctxt);
}

void CypherAST::addVarDecl(VarDecl* decl) {
    _varDecls.push_back(decl);
}

void CypherAST::addNodePatternData(NodePatternData* data) {
    _nodePatternDatas.push_back(data);
}

void CypherAST::addEdgePatternData(EdgePatternData* data) {
    _edgePatternDatas.push_back(data);
}
