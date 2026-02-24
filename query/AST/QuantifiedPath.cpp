#include "QuantifiedPath.h"

#include "CypherAST.h"

using namespace db;

QuantifiedPath::QuantifiedPath()
{
}

QuantifiedPath::~QuantifiedPath() {
}

QuantifiedPath* QuantifiedPath::create(CypherAST* ast) {
    QuantifiedPath* p = new QuantifiedPath();
    ast->addQuantifiedPath(p);
    return p;
}
