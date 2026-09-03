#include "LoadCSVStmt.h"

#include <algorithm>

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

size_t LoadCSVStmt::declareField(size_t index) {
    const auto samePosition = [index](const Field& field) {
        return !field._byHeader && field._index == index;
    };

    const auto foundIt = std::ranges::find_if(_fields, samePosition);
    if (foundIt != end(_fields)) {
        return static_cast<size_t>(std::distance(begin(_fields), foundIt));
    }

    _fields.push_back(Field {._index = index});

    return _fields.size() - 1;
}

size_t LoadCSVStmt::declareField(std::string_view header) {
    const auto sameHeader = [header](const Field& field) {
        return field._byHeader && field._header == header;
    };

    const auto foundIt = std::ranges::find_if(_fields, sameHeader);
    if (foundIt != end(_fields)) {
        return static_cast<size_t>(std::distance(begin(_fields), foundIt));
    }

    _fields.push_back(Field {._header = header, ._byHeader = true});

    return _fields.size() - 1;
}
