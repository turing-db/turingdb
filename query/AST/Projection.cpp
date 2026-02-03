#include "Projection.h"

#include "CypherAST.h"

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
