#include "Projection.h"

#include "CypherAST.h"

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
