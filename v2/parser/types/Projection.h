#pragma once

#include <memory>
#include <variant>
#include <vector>

#include "ParserException.h"

namespace db {

class Limit;
class Skip;
class Expression;

class Projection {
public:
    struct All {};

    using ItemVector = std::vector<Expression*>;

    Projection() = default;
    ~Projection() = default;

    static std::unique_ptr<Projection> create() {
        return std::make_unique<Projection>();
    }

    Projection(const Projection&) = delete;
    Projection& operator=(const Projection&) = delete;
    Projection(Projection&&) = delete;
    Projection& operator=(Projection&&) = delete;

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

    const std::vector<Expression*>& getItems() const {
        return std::get<std::vector<Expression*>>(_items);
    }

    void add(Expression* expression) {
        auto* items = std::get_if<ItemVector>(&_items);
        if (!items) {
            throw ParserException("Cannot add item to a project that already holds '*'");
        }

        items->emplace_back(expression);
    }

    void setAll() {
        _items = All {};
    }

private:
    Limit* _limit {nullptr};
    Skip* _skip {nullptr};

    bool _distinct {false};

    std::variant<ItemVector, All> _items;
};

}
