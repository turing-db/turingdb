#pragma once

#include <optional>
#include <stddef.h>
#include <stdint.h>
#include <list>
#include <vector>
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
    using PublishedDecls = std::vector<VarDecl*>;

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

    void addExpr(Expr* expr);
    void pushFrontDecl(VarDecl* decl);

    void setReturnAll() { _returningAll = true; }

    std::optional<std::string_view> getName(const Expr* item) const;
    std::optional<std::string_view> getName(const VarDecl* item) const;
    bool hasName(const std::string_view& name) const;

    // The variable each item declares in the scope a WITH opens, in item order. What
    // follows the barrier reads the published columns through these declarations rather
    // than through the ones the items carry, which belong to the scope above it.
    const PublishedDecls& publishedDecls() const { return _publishedDecls; }
    void addPublishedDecl(VarDecl* decl) { _publishedDecls.push_back(decl); }

    size_t findItemIndex(const Expr* key) const;
    const Expr* findItemExpr(const Expr* key) const;
    bool hasItem(const Expr* key) const;
    bool hasVariableItem(const VarDecl* decl) const;

private:
    Limit* _limit {nullptr};
    Skip* _skip {nullptr};
    OrderBy* _orderBy {nullptr};
    bool _distinct {false};
    bool _aggregate {false};
    bool _hasGroupingKeys {false};
    bool _returningAll {false};

    Items _items;

    PublishedDecls _publishedDecls;

    // Maps a VarDecl*/Expr* to its column name
    std::unordered_map<uintptr_t, std::string_view> _names;
    std::unordered_set<std::string_view> _namesSet;

    Items::const_iterator locateItem(const Expr* key) const;

    Projection();
    ~Projection();
};

}
