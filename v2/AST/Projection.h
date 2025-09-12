#pragma once

#include <variant>
#include <vector>

#include "ASTException.h"

namespace db::v2 {

class Limit;
class Skip;
class Expr;
class CypherAST;

class Projection {
public:
    friend CypherAST;
    struct All {};

    using ItemVector = std::vector<Expr*>;

    static Projection* create(CypherAST* ast);

    bool isDistinct() const {
        return _distinct;
    }

    bool hasLimit() const {
        return _limit;
    }

    bool hasSkip() const {
        return _skip;
    }

    void setDistinct(bool distinct) {
        _distinct = distinct;
    }

    void setLimit(Limit* limit) {
        _limit = limit;
    }

    void setSkip(Skip* skip) {
        _skip = skip;
    }

    bool isAll() const {
        return std::holds_alternative<All>(_items);
    }

    const Limit& limit() const {
        return *_limit;
    }

    const Skip& skip() const {
        return *_skip;
    }

    const std::vector<Expr*>& items() const {
        return std::get<std::vector<Expr*>>(_items);
    }

    void add(Expr* Expr) {
        auto* items = std::get_if<ItemVector>(&_items);
        if (!items) {
            throw ASTException("Cannot add item to a projection that already holds '*'");
        }

        items->emplace_back(Expr);
    }

    void setAll() {
        _items = All {};
    }

private:
    Limit* _limit {nullptr};
    Skip* _skip {nullptr};

    bool _distinct {false};

    std::variant<ItemVector, All> _items;

    Projection();
    ~Projection();
};

}
