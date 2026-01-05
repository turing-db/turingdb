#pragma once

namespace db {

class CypherAST;
class YieldItems;

class YieldClause {
public:
    friend CypherAST;

    static YieldClause* create(CypherAST* ast);

    void setItems(YieldItems* items) { _items = items; }

    YieldItems* getItems() const { return _items; }

private:
    YieldItems* _items {nullptr};

    YieldClause()
    {
    }

    ~YieldClause() = default;
};

}
