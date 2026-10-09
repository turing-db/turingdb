#pragma once

#include <vector>

namespace db {

class CypherAST;
class Parameter;
class Symbol;

class SymbolChain {
public:
    friend CypherAST;

    using SymbolVector = std::vector<Symbol*>;
    using ParameterVector = std::vector<Parameter*>;

    static SymbolChain* create(CypherAST* ast);

    void add(Symbol* symbol);
    void addParameter(Parameter* parameter);

    const SymbolVector& getVector() const { return _symbols; }
    const ParameterVector& getParameters() const { return _parameters; }

    SymbolVector::const_iterator begin() const { return _symbols.begin(); }
    SymbolVector::const_iterator end() const { return _symbols.end(); }

    bool empty() const { return _symbols.empty() && _parameters.empty(); }
    size_t size() const { return _symbols.size(); }
    Symbol* front() const { return _symbols.front(); }

private:
    std::vector<Symbol*> _symbols;
    ParameterVector _parameters;

    SymbolChain();
    ~SymbolChain();
};

}
