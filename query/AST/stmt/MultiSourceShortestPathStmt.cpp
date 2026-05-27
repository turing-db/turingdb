#include "MultiSourceShortestPathStmt.h"

#include "CypherAST.h"

using namespace db;

MultiSourceShortestPathStmt::MultiSourceShortestPathStmt(Symbol* source,
                                                         Symbol* target,
                                                         Symbol* edgeProperty,
                                                         Symbol* sourceVar,
                                                         Symbol* targetVar,
                                                         Symbol* distVar,
                                                         Symbol* pathVar)
    : _source(source),
    _target(target),
    _edgeProperty(edgeProperty),
    _sourceVar(sourceVar),
    _targetVar(targetVar),
    _distVar(distVar),
    _pathVar(pathVar)
{
}

MultiSourceShortestPathStmt::~MultiSourceShortestPathStmt() {
}

MultiSourceShortestPathStmt* MultiSourceShortestPathStmt::create(CypherAST* ast,
                                                                 Symbol* source,
                                                                 Symbol* target,
                                                                 Symbol* edgeProperty,
                                                                 Symbol* sourceVar,
                                                                 Symbol* targetVar,
                                                                 Symbol* distVar,
                                                                 Symbol* pathVar) {
    MultiSourceShortestPathStmt* stmt = new MultiSourceShortestPathStmt(source,
                                                                        target,
                                                                        edgeProperty,
                                                                        sourceVar,
                                                                        targetVar,
                                                                        distVar,
                                                                        pathVar);
    ast->addStmt(stmt);
    return stmt;
}
