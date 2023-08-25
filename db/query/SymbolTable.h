#pragma once

#include <unordered_map>
#include <vector>
#include <string>

namespace db::query {

class Symbol;

class SymbolTable {
public:
    SymbolTable();
    ~SymbolTable();

    size_t size() const { return _symbols.size(); }

    Symbol* createSymbol(const std::string& name);

private:
    std::vector<Symbol*> _symbols;
    std::unordered_map<std::string, Symbol*> _nameMap;
};

}
