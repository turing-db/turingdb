#include "CypherAST.h"

#include <algorithm>

#include "BioAssert.h"
#include "DiagnosticsManager.h"
#include "SourceManager.h"
#include "FunctionInvocation.h"
#include "Parameter.h"
#include "Symbol.h"
#include "SymbolChain.h"
#include "LabelExpr.h"
#include "Literal.h"
#include "QualifiedName.h"
#include "QuantifiedPath.h"
#include "expr/ExprChain.h"
#include "expr/Expr.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "EntityPattern.h"
#include "Projection.h"
#include "WhereClause.h"
#include "YieldClause.h"
#include "YieldItems.h"
#include "ProcedureLookup.h"
#include "stmt/Stmt.h"
#include "stmt/SubStmt.h"
#include "stmt/StmtContainer.h"
#include "stmt/OrderByItem.h"
#include "stmt/SetItem.h"
#include "ExplainRequest.h"
#include "QueryCommand.h"
#include "decl/DeclContext.h"
#include "decl/VarDecl.h"
#include "decl/ParameterDecl.h"
#include "decl/PatternData.h"

using namespace db;

CypherAST::CypherAST(const ProcedureManager* procedures,
                     std::string_view queryString)
    : _sourceManager(new SourceManager(queryString)),
    _diagnosticsManager(new DiagnosticsManager(_sourceManager)),
    _procedureLookup(std::make_unique<ProcedureLookup>(procedures))
{
}

ExplainRequest& CypherAST::explainRequest() {
    if (!_explainRequest) {
        _explainRequest = std::make_unique<ExplainRequest>();
    }

    return *_explainRequest;
}

CypherAST::~CypherAST() {
    delete _sourceManager;
    delete _diagnosticsManager;

    for (Parameter* parameter : _parameters) {
        delete parameter;
    }

    for (Symbol* symbol : _symbols) {
        delete symbol;
    }

    for (SymbolChain* chain : _symbolChains) {
        delete chain;
    }

    for (LabelExpr* expr : _labelExprs) {
        delete expr;
    }

    for (QualifiedName* name : _qualifiedNames) {
        delete name;
    }

    for (QuantifiedPath* path: _quantifiedPaths) {
        delete path;
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

    for (YieldClause* clause : _yieldClauses) {
        delete clause;
    }

    for (YieldItems* items: _yieldItems) {
        delete items;
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

    for (SetItem* item : _setItems) {
        delete item;
    }

    for (StmtContainer* container : _stmtContainers) {
        delete container;
    }

    for (QueryCommand* query : _queries) {
        delete query;
    }

    for (QueryCommand* query : _subQueries) {
        delete query;
    }

    for (DeclContext* ctxt : _declContexts) {
        delete ctxt;
    }

    for (VarDecl* decl : _varDecls) {
        delete decl;
    }

    for (ParameterDecl* decl : _parameterDecls) {
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

    for (FunctionInvocation* invocation : _functionInvocations) {
        delete invocation;
    }
}

std::string* CypherAST::createString() {
    std::string* name = new std::string();
    _unnamedVarIdentifiers.push_back(name);

    return name;
}

void CypherAST::addParameter(Parameter* parameter) {
    _parameters.push_back(parameter);
}

void CypherAST::addSymbol(Symbol* symbol) {
    _symbols.push_back(symbol);
}

void CypherAST::addSymbolChain(SymbolChain* symbol) {
    _symbolChains.push_back(symbol);
}

void CypherAST::addLabelExpr(LabelExpr* expr) {
    _labelExprs.push_back(expr);
}

void CypherAST::addQualifiedName(QualifiedName* name) {
    _qualifiedNames.push_back(name);
}

void CypherAST::addQuantifiedPath(QuantifiedPath* path) {
    _quantifiedPaths.push_back(path);
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

void CypherAST::addYieldClause(YieldClause* clause) {
    _yieldClauses.push_back(clause);
}

void CypherAST::addYieldItems(YieldItems* items) {
    _yieldItems.push_back(items);
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

void CypherAST::addSetItem(SetItem* item) {
    _setItems.push_back(item);
}

void CypherAST::addStmtContainer(StmtContainer* container) {
    _stmtContainers.push_back(container);
}

void CypherAST::addQuery(QueryCommand* query) {
    _queries.push_back(query);
}

void CypherAST::nestQuery(QueryCommand* query) {
    const auto findIt = std::find(_queries.begin(), _queries.end(), query);
    bioassert(findIt != _queries.end(), "Nesting a command the AST does not hold");

    _queries.erase(findIt);
    _subQueries.push_back(query);
}

void CypherAST::saveCheckpoint(Checkpoint& checkpoint) const {
    checkpoint._parameters = _parameters.size();
    checkpoint._symbols = _symbols.size();
    checkpoint._symbolChains = _symbolChains.size();
    checkpoint._labelExprs = _labelExprs.size();
    checkpoint._qualifiedNames = _qualifiedNames.size();
    checkpoint._quantifiedPaths = _quantifiedPaths.size();
    checkpoint._functionInvocations = _functionInvocations.size();
    checkpoint._literals = _literals.size();
    checkpoint._expressions = _expressions.size();
    checkpoint._exprChains = _exprChains.size();
    checkpoint._patterns = _patterns.size();
    checkpoint._patternElems = _patternElems.size();
    checkpoint._entityPatterns = _entityPatterns.size();
    checkpoint._projections = _projections.size();
    checkpoint._whereClauses = _whereClauses.size();
    checkpoint._yieldClauses = _yieldClauses.size();
    checkpoint._yieldItems = _yieldItems.size();
    checkpoint._stmts = _stmts.size();
    checkpoint._subStmts = _subStmts.size();
    checkpoint._setItems = _setItems.size();
    checkpoint._orderByItems = _orderByItems.size();
    checkpoint._stmtContainers = _stmtContainers.size();
    checkpoint._queries = _queries.size();
    checkpoint._subQueries = _subQueries.size();
    checkpoint._declContexts = _declContexts.size();
    checkpoint._varDecls = _varDecls.size();
    checkpoint._parameterDecls = _parameterDecls.size();
    checkpoint._nodePatternDatas = _nodePatternDatas.size();
    checkpoint._edgePatternDatas = _edgePatternDatas.size();
    checkpoint._unnamedVarIdentifiers = _unnamedVarIdentifiers.size();
}

template <typename Object>
void CypherAST::truncateObjects(std::vector<Object*>& objects, size_t count) {
    for (size_t index = count; index < objects.size(); index++) {
        _sourceManager->eraseLocation(objects[index]);
        delete objects[index];
    }

    objects.resize(count);
}

void CypherAST::rollback(const Checkpoint& checkpoint) {
    for (size_t index = checkpoint._parameterDecls; index < _parameterDecls.size(); index++) {
        _parameterDeclMap.erase(_parameterDecls[index]->getName());
    }

    truncateObjects(_parameters, checkpoint._parameters);
    truncateObjects(_symbols, checkpoint._symbols);
    truncateObjects(_symbolChains, checkpoint._symbolChains);
    truncateObjects(_labelExprs, checkpoint._labelExprs);
    truncateObjects(_qualifiedNames, checkpoint._qualifiedNames);
    truncateObjects(_quantifiedPaths, checkpoint._quantifiedPaths);
    truncateObjects(_functionInvocations, checkpoint._functionInvocations);
    truncateObjects(_literals, checkpoint._literals);
    truncateObjects(_expressions, checkpoint._expressions);
    truncateObjects(_exprChains, checkpoint._exprChains);
    truncateObjects(_patterns, checkpoint._patterns);
    truncateObjects(_patternElems, checkpoint._patternElems);
    truncateObjects(_entityPatterns, checkpoint._entityPatterns);
    truncateObjects(_projections, checkpoint._projections);
    truncateObjects(_whereClauses, checkpoint._whereClauses);
    truncateObjects(_yieldClauses, checkpoint._yieldClauses);
    truncateObjects(_yieldItems, checkpoint._yieldItems);
    truncateObjects(_stmts, checkpoint._stmts);
    truncateObjects(_subStmts, checkpoint._subStmts);
    truncateObjects(_setItems, checkpoint._setItems);
    truncateObjects(_orderByItems, checkpoint._orderByItems);
    truncateObjects(_stmtContainers, checkpoint._stmtContainers);
    truncateObjects(_queries, checkpoint._queries);
    truncateObjects(_subQueries, checkpoint._subQueries);
    truncateObjects(_declContexts, checkpoint._declContexts);
    truncateObjects(_varDecls, checkpoint._varDecls);
    truncateObjects(_parameterDecls, checkpoint._parameterDecls);
    truncateObjects(_nodePatternDatas, checkpoint._nodePatternDatas);
    truncateObjects(_edgePatternDatas, checkpoint._edgePatternDatas);
    truncateObjects(_unnamedVarIdentifiers, checkpoint._unnamedVarIdentifiers);
}

void CypherAST::addDeclContext(DeclContext* ctxt) {
    _declContexts.push_back(ctxt);
}

void CypherAST::addVarDecl(VarDecl* decl) {
    _varDecls.push_back(decl);
}

void CypherAST::addParameterDecl(ParameterDecl* decl) {
    _parameterDecls.push_back(decl);
    _parameterDeclMap[decl->getName()] = decl;
}

ParameterDecl* CypherAST::findParameterDecl(std::string_view name) {
    const auto it = _parameterDeclMap.find(name);
    return it != _parameterDeclMap.end() ? it->second : nullptr;
}

void CypherAST::addNodePatternData(NodePatternData* data) {
    _nodePatternDatas.push_back(data);
}

void CypherAST::addEdgePatternData(EdgePatternData* data) {
    _edgePatternDatas.push_back(data);
}

void CypherAST::addFunctionInvocation(FunctionInvocation* invocation) {
    _functionInvocations.push_back(invocation);
}
