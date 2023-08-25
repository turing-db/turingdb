#include "SymbolTable.h"

#include "Symbol.h"

using namespace db::query;

SymbolTable::SymbolTable()
{
}

SymbolTable::~SymbolTable() {
}

Symbol* SymbolTable::createSymbol(const std::string& name) {
    const auto it = _nameMap.find(name);
    if (it != _nameMap.end()) {
        return it->second;
    }

    const size_t pos = _symbols.size();
    Symbol* sym = new Symbol(name, pos);
    _nameMap[name] = sym;
    _symbols.push_back(sym);
    return sym;
}
