#include "VectorSearchStmt.h"

#include "CypherAST.h"

using namespace db;

VectorSearchStmt::VectorSearchStmt()
{
}

VectorSearchStmt::~VectorSearchStmt() {
}

VectorSearchStmt* VectorSearchStmt::create(CypherAST* ast) {
    VectorSearchStmt* stmt = new VectorSearchStmt();
    ast->addStmt(stmt);
    return stmt;
}
