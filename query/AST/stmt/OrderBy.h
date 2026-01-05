#pragma once

#include <vector>

#include "SubStmt.h"

namespace db::v2 {

class OrderByItem;

class OrderBy : public SubStmt {
public:
    using ItemVector = std::vector<OrderByItem*>;

    static OrderBy* create(CypherAST* ast);

    const ItemVector& getItems() const { return _items; }

    void addItem(OrderByItem* item) { _items.push_back(item); }

private:
    ItemVector _items;

    OrderBy();
    ~OrderBy() override;
};

}
