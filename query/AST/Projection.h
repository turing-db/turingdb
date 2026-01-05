#pragma once

#include <variant>
#include <vector>

#include "CompilerException.h"

namespace db::v2 {

class Limit;
class Skip;
class OrderBy;
class Expr;
class CypherAST;

class Projection {
public:
    friend CypherAST;
    struct All {};
    using Items = std::vector<Expr*>;

    static Projection* create(CypherAST* ast);

    bool isDistinct() const { return _distinct; }

    bool hasOrderBy() const { return _orderBy; }

    bool hasLimit() const { return _limit; }

    bool hasSkip() const { return _skip; }

    bool isAggregate() const { return _aggregate; }

    bool hasGroupingKeys() const { return _hasGroupingKeys; }

    void setDistinct(bool distinct) { _distinct = distinct; }

    void setLimit(Limit* limit) { _limit = limit; }

    void setSkip(Skip* skip) { _skip = skip; }

    void setOrderBy(OrderBy* orderBy) { _orderBy = orderBy; }

    void setAggregate(bool aggregate = true) { _aggregate = aggregate; }

    void setHasGroupingKeys(bool hasGroupingKeys = true) { _hasGroupingKeys = hasGroupingKeys; }

    bool isAll() const {
        return std::holds_alternative<All>(_items);
    }

    Limit* getLimit() const { return _limit; }
    Skip* getSkip() const { return _skip; }
    OrderBy* getOrderBy() const { return _orderBy; }

    const Items& items() const {
        return std::get<Items>(_items);
    }

    void add(Expr* Expr) {
        auto* items = std::get_if<Items>(&_items);
        if (!items) {
            throw CompilerException("Cannot add item to a projection that already holds '*'");
        }

        items->emplace_back(Expr);
    }

    void setAll() { _items = All {}; }

private:
    Limit* _limit {nullptr};
    Skip* _skip {nullptr};
    OrderBy* _orderBy {nullptr};
    bool _distinct {false};
    bool _aggregate {false};
    bool _hasGroupingKeys {false};

    std::variant<Items, All> _items;

    Projection();
    ~Projection();
};

}
