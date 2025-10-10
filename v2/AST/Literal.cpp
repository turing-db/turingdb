#include "Literal.h"

#include "CypherAST.h"

using namespace db::v2;

NullLiteral* NullLiteral::create(CypherAST* ast) {
    NullLiteral* literal = new NullLiteral();
    ast->addLiteral(literal);
    return literal;
}

BoolLiteral* BoolLiteral::create(CypherAST* ast, bool value) {
    BoolLiteral* literal = new BoolLiteral(value);
    ast->addLiteral(literal);
    return literal;
}

IntegerLiteral* IntegerLiteral::create(CypherAST* ast, int64_t value) {
    IntegerLiteral* literal = new IntegerLiteral(value);
    ast->addLiteral(literal);
    return literal;
}

DoubleLiteral* DoubleLiteral::create(CypherAST* ast, double value) {
    DoubleLiteral* literal = new DoubleLiteral(value);
    ast->addLiteral(literal);
    return literal;
}

StringLiteral* StringLiteral::create(CypherAST* ast, std::string_view value) {
    StringLiteral* literal = new StringLiteral(value);
    ast->addLiteral(literal);
    return literal;
}

CharLiteral* CharLiteral::create(CypherAST* ast, char value) {
    CharLiteral* literal = new CharLiteral(value);
    ast->addLiteral(literal);
    return literal;
}

MapLiteral::MapLiteral()
{
}

MapLiteral::~MapLiteral() {
}

MapLiteral* MapLiteral::create(CypherAST* ast) {
    MapLiteral* literal = new MapLiteral();
    ast->addLiteral(literal);
    return literal;
}

void MapLiteral::set(Symbol* key, Expr* value) {
    _map[key] = value;
}
