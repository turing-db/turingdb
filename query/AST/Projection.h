#pragma once

#include <list>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <variant>

namespace db {

class Limit;
class Skip;
class OrderBy;
class Expr;
class VarDecl;
class CypherAST;

class Projection {
public:
    friend CypherAST;
    struct All {};

    using ReturnItem = std::variant<Expr*, VarDecl*>;
    using Items = std::list<ReturnItem>;

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

    void setName(const Expr* item, std::string_view name);
    void setName(const VarDecl* item, std::string_view name);

    bool isReturningAll() const {
        return _returningAll;
    }

    Limit* getLimit() const { return _limit; }
    Skip* getSkip() const { return _skip; }
    OrderBy* getOrderBy() const { return _orderBy; }

    const Items& items() const {
        return _items;
    }

    void pushBackExpr(Expr* expr);
    void pushFrontDecl(VarDecl* decl);

    void setReturnAll() { _returningAll = true; }

    const std::string_view* getName(const Expr* item) const;
    const std::string_view* getName(const VarDecl* item) const;
    bool hasName(const std::string_view& name) const;

private:
    Limit* _limit {nullptr};
    Skip* _skip {nullptr};
    OrderBy* _orderBy {nullptr};
    bool _distinct {false};
    bool _aggregate {false};
    bool _hasGroupingKeys {false};
    bool _returningAll {false};

    Items _items;
    std::unordered_map<std::uintptr_t, std::string_view> _names;
    std::unordered_set<std::string_view> _namesSet;

    Projection();
    ~Projection();
};

}
