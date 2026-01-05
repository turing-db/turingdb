#pragma once

#include <vector>

namespace db::v2 {

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

    void addName(Symbol* symbol);

private:
    Symbols _names;

    QualifiedName();
    ~QualifiedName();
};

}
