#include "LoadCSVStmt.h"

#include "CypherAST.h"

using namespace db;

LoadCSVStmt::LoadCSVStmt(std::string_view filePath,
                         Symbol* alias)
    : _filePath(fs::Path(std::string(filePath))),
    _alias(alias)
{
}

LoadCSVStmt::~LoadCSVStmt() = default;

LoadCSVStmt* LoadCSVStmt::create(CypherAST* ast,
                                 std::string_view filePath,
                                 Symbol* alias) {
    LoadCSVStmt* stmt = new LoadCSVStmt(filePath, alias);
    ast->addStmt(stmt);
    return stmt;
}
