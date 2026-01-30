#include "ShortestPathStmt.h"

#include "CypherAST.h"

using namespace db;

ShortestPathStmt::ShortestPathStmt(Symbol* source,
                                   Symbol* target,
                                   Symbol* edgeProperty,
                                   Symbol* distVar,
                                   Symbol* pathVar)
    : _source(source),
    _target(target),
    _edgeProperty(edgeProperty),
    _distVar(distVar),
    _pathVar(pathVar)
{
}

ShortestPathStmt::~ShortestPathStmt() {
}

ShortestPathStmt* ShortestPathStmt::create(CypherAST* ast,
                                           Symbol* source,
                                           Symbol* target,
                                           Symbol* edgeProperty,
                                           Symbol* distVar,
                                           Symbol* pathVar) {
    ShortestPathStmt* stmt = new ShortestPathStmt(source,
                                                  target,
                                                  edgeProperty,
                                                  distVar,
                                                  pathVar);
    ast->addStmt(stmt);
    return stmt;
}
