#include "EntityPattern.h"

#include "CypherAST.h"

using namespace db::v2;

EntityPattern* EntityPattern::create(CypherAST* ast) {
    EntityPattern* pattern = new EntityPattern();
    ast->addEntityPattern(pattern);
    return pattern;
}
