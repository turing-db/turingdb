#include "Symbol.h"

#include "CypherAST.h"

using namespace db::v2;

Symbol* Symbol::create(CypherAST* ast, std::string_view name) {
    Symbol* symbol = new Symbol(name);
    ast->addSymbol(symbol);
    return symbol;
}
