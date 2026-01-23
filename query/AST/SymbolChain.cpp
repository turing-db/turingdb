#include "SymbolChain.h"

#include "Symbol.h"
#include "CypherAST.h"

using namespace db;

SymbolChain::SymbolChain()
{
}

SymbolChain::~SymbolChain() {
}

SymbolChain* SymbolChain::create(CypherAST* ast) {
    SymbolChain* chain = new SymbolChain();
    ast->addSymbolChain(chain);
    return chain;
}

void SymbolChain::add(Symbol* symbol) {
    _symbols.push_back(symbol);
}
