#include "Projection.h"

#include <iterator>

#include "CypherAST.h"
#include "expr/Expr.h"
#include "expr/StructuralExpressionComparator.h"

using namespace db;

Projection::Projection()
{
}

Projection::~Projection() {
}

Projection* Projection::create(CypherAST* ast) {
    Projection* projection = new Projection();
    ast->addProjection(projection);
    return projection;
}

void Projection::setName(const Expr* item, std::string_view name) {
    _names[std::bit_cast<uintptr_t>(item)] = name;
    _namesSet.emplace(name);
}

void Projection::setName(const VarDecl* item, std::string_view name) {
    _names[std::bit_cast<uintptr_t>(item)] = name;
    _namesSet.emplace(name);
}

void Projection::addExpr(Expr* expr) {
    _items.emplace_back(expr);
}

void Projection::pushFrontDecl(VarDecl* decl) {
    _items.emplace_front(decl);
}

std::optional<std::string_view> Projection::getName(const Expr* item) const {
    auto it = _names.find(std::bit_cast<uintptr_t>(item));
    if (it == end(_names)) {
        return std::nullopt;
    }

    return it->second;
}

std::optional<std::string_view> Projection::getName(const VarDecl* item) const {
    auto it = _names.find(std::bit_cast<uintptr_t>(item));
    if (it == end(_names)) {
        return std::nullopt;
    }

    return it->second;
}

bool Projection::hasName(const std::string_view& name) const {
    return _namesSet.contains(name);
}

Projection::Items::const_iterator Projection::locateItem(const Expr* key) const {
    const VarDecl* keyDecl = key->getExprVarDecl();
    const Items::const_iterator itemsEnd = _items.end();

    for (Items::const_iterator item = _items.begin(); item != itemsEnd; item++) {
        if (const Expr* const* itemExpr = std::get_if<Expr*>(&*item)) {
            const Expr* projectedExpr = *itemExpr;

            // A key may also name the alias an item was given - the x of
            // RETURN n.name AS x ORDER BY x - which is one variable declared once, so the
            // key and the item share its declaration. Every other expression is given a
            // declaration of its own, so only an alias is ever matched here
            const bool namesTheItem = keyDecl && projectedExpr->getExprVarDecl() == keyDecl;
            const bool readsTheItem = StructuralExpressionComparator::equal(projectedExpr, key);

            if (namesTheItem || readsTheItem) {
                return item;
            }
        } else if (const VarDecl* const* itemDecl = std::get_if<VarDecl*>(&*item)) {
            const bool namesTheItem = keyDecl && *itemDecl == keyDecl;

            if (namesTheItem) {
                return item;
            }
        }
    }

    return itemsEnd;
}

size_t Projection::findItemIndex(const Expr* key) const {
    return static_cast<size_t>(std::distance(_items.begin(), locateItem(key)));
}

const Expr* Projection::findItemExpr(const Expr* key) const {
    const Items::const_iterator item = locateItem(key);
    if (item == _items.end()) {
        return nullptr;
    }

    const Expr* const* itemExpr = std::get_if<Expr*>(&*item);
    if (!itemExpr) {
        return nullptr;
    }

    return *itemExpr;
}

bool Projection::hasItem(const Expr* key) const {
    return locateItem(key) != _items.end();
}

bool Projection::hasVariableItem(const VarDecl* decl) const {
    if (!decl) {
        return false;
    }

    for (const ReturnItem& item : _items) {
        if (const Expr* const* itemExpr = std::get_if<Expr*>(&item)) {
            const Expr* projectedExpr = *itemExpr;

            // Only a bare variable returns the variable itself; every other item returns
            // a value computed from it, which leaves the variable behind
            const bool isBareVariable = projectedExpr->getKind() == Expr::Kind::SYMBOL;

            if (isBareVariable && projectedExpr->getExprVarDecl() == decl) {
                return true;
            }
        } else if (const VarDecl* const* itemDecl = std::get_if<VarDecl*>(&item)) {
            if (*itemDecl == decl) {
                return true;
            }
        }
    }

    return false;
}
