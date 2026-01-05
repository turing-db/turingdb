#include "Projection.h"

#include "CypherAST.h"

#include "CompilerException.h"

using namespace db::v2;

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

void Projection::add(Expr* expr) {
    auto* items = std::get_if<Items>(&_items);
    if (!items) {
        throw CompilerException("Cannot add item to a projection that already holds '*'");
    }

    items->emplace_back(expr);
}
