#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class OrderByItem;
class Expr;

class OrderByNode : public PlanGraphNode {
public:
    using ItemVector = std::vector<OrderByItem*>;

    explicit OrderByNode()
        : PlanGraphNode(PlanGraphOpcode::ORDER_BY)
    {
    }

    void setItems(const ItemVector& items) {
        _items = items;
    }

    const ItemVector& items() const {
        return _items;
    }

private:
    ItemVector _items;
};

}
