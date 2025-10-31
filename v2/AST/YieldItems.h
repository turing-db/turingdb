#pragma once

#include <vector>

namespace db::v2 {

class CypherAST;
class Symbol;
class WhereClause;

class YieldItems {
public:
    using ItemVector = std::vector<Symbol*>;

    ~YieldItems();

    static YieldItems* create(CypherAST* ast);

    void add(Symbol* symbol) { _items.push_back(symbol); }
    void setWhere(WhereClause* whereClause) { _whereClause = whereClause; }

    const ItemVector& getItems() const { return _items; }
    WhereClause* getWhereClause() const { return _whereClause; }

    ItemVector::const_iterator begin() const { return _items.begin(); }
    ItemVector::const_iterator end() const { return _items.end(); }

private:
    ItemVector _items;
    WhereClause* _whereClause {nullptr};

    YieldItems();
};

}
