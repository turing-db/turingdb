#pragma once

#include <unordered_map>
#include <vector>
#include <string>

namespace db {

class Symbol;

class SymbolTable {
public:
    using Symbols = std::vector<Symbol*>;

    SymbolTable();
    ~SymbolTable();

    size_t size() const { return _symbols.size(); }

    const Symbols& symbols() const { return _symbols; }

    Symbol* createSymbol(const std::string& name);

private:
    Symbols _symbols;
    std::unordered_map<std::string, Symbol*> _nameMap;
};

}
