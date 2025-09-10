#pragma once

#include <vector>

namespace db::v2 {

class PatternElement;
class WhereClause;
class CypherAST;

class Pattern {
public:
    friend CypherAST;
    using PatternElements = std::vector<PatternElement*>;

    static Pattern* create(CypherAST* ast);

    void setWhere(WhereClause* where) { _where = where; }

    WhereClause* getWhere() const { return _where; }

    void addElement(PatternElement* element) { _elems.push_back(element); }

    const PatternElements& elements() const { return _elems; }

private:
    PatternElements _elems;
    WhereClause* _where {nullptr};

    Pattern() = default;
    ~Pattern();
};

}
