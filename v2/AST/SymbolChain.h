#pragma once

#include <vector>

namespace db::v2 {

class CypherAST;
class Symbol;

class SymbolChain {
public:
    using SymbolVector = std::vector<Symbol*>;

    ~SymbolChain();

    static SymbolChain* create(CypherAST* ast);

    void add(Symbol* symbol);

    SymbolChain(const SymbolChain&) = delete;
    SymbolChain(SymbolChain&&) = delete;
    SymbolChain& operator=(const SymbolChain&) = delete;
    SymbolChain& operator=(SymbolChain&&) = delete;

    const SymbolVector& getVector() const { return _symbols; }

    SymbolVector::const_iterator begin() const { return _symbols.begin(); }
    SymbolVector::const_iterator end() const { return _symbols.end(); }

    bool empty() const { return _symbols.empty(); }
    size_t size() const { return _symbols.size(); }
    Symbol* front() const { return _symbols.front(); }

private:
    std::vector<Symbol*> _symbols;

    SymbolChain();
};

}
