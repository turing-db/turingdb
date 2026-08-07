#include "Projection.h"

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

size_t Projection::findItemIndex(const Expr* key) const {
    const VarDecl* keyDecl = key->getExprVarDecl();

    size_t index = 0;
    for (const ReturnItem& item : _items) {
        if (const Expr* const* itemExpr = std::get_if<Expr*>(&item)) {
            const Expr* projectedExpr = *itemExpr;

            // A key may also name the alias an item was given - the x of
            // RETURN n.name AS x ORDER BY x - which is one variable declared once, so the
            // key and the item share its declaration. Every other expression is given a
            // declaration of its own, so only an alias is ever matched here
            const bool namesTheItem = keyDecl && projectedExpr->getExprVarDecl() == keyDecl;
            const bool readsTheItem = StructuralExpressionComparator::equal(projectedExpr, key);

            if (namesTheItem || readsTheItem) {
                return index;
            }
        } else if (const VarDecl* const* itemDecl = std::get_if<VarDecl*>(&item)) {
            const bool namesTheItem = keyDecl && *itemDecl == keyDecl;

            if (namesTheItem) {
                return index;
            }
        }

        index++;
    }

    return index;
}

bool Projection::hasItem(const Expr* key) const {
    return findItemIndex(key) < _items.size();
}
