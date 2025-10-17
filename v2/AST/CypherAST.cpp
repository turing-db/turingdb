#include "CypherAST.h"

#include "DiagnosticsManager.h"
#include "SourceManager.h"
#include "Symbol.h"
#include "QualifiedName.h"
#include "expr/ExprChain.h"
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
#include "stmt/OrderByItem.h"
#include "QueryCommand.h"
#include "decl/DeclContext.h"
#include "decl/VarDecl.h"
#include "decl/PatternData.h"

using namespace db::v2;

CypherAST::CypherAST(std::string_view queryString)
    : _sourceManager(new SourceManager(queryString)),
    _diagnosticsManager(new DiagnosticsManager(_sourceManager))
{
}

CypherAST::~CypherAST() {
    delete _sourceManager;
    delete _diagnosticsManager;

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

    for (OrderByItem* item : _orderByItems) {
        delete item;
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

    for (std::string* name : _unnamedVarIdentifiers) {
        delete name;
    }

    for (ExprChain* chain : _exprChains) {
        delete chain;
    }
}

std::string* CypherAST::createString() {
    std::string* name = new std::string();
    _unnamedVarIdentifiers.push_back(name);

    return name;
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

void CypherAST::addExprChain(ExprChain* exprChain) {
    _exprChains.push_back(exprChain);
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

void CypherAST::addOrderByItem(OrderByItem* item) {
    _orderByItems.push_back(item);
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
