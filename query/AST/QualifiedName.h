#pragma once

#include <vector>

namespace db {

class Symbol;   
class CypherAST;

class QualifiedName {
public:
    friend CypherAST;
    using Symbols = std::vector<Symbol*>;

    static QualifiedName* create(CypherAST* ast);

    const Symbols& names() const { return _names; }

    size_t size() const { return _names.size(); }

    Symbol* get(size_t index) const { return _names[index]; }

    Symbol* front() const { return _names.front(); }
    Symbol* back() const { return _names.back(); }

    void addName(Symbol* symbol);

private:
    Symbols _names;

    QualifiedName();
    ~QualifiedName();
};

}
