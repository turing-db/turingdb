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

    const PatternElements& elements() const { return _elems; }
    WhereClause* getWhere() const { return _where; }

    void setWhere(WhereClause* where) { _where = where; }
    void addElement(PatternElement* element) { _elems.push_back(element); }

private:
    PatternElements _elems;
    WhereClause* _where {nullptr};

    Pattern();
    ~Pattern();
};

}
