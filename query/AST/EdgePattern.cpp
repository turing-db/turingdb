#include "EdgePattern.h"

#include "CypherAST.h"

using namespace db::v2;

EdgePattern::EdgePattern(Direction direction)
    : _direction(direction)
{
}

EdgePattern::~EdgePattern() {
}

EdgePattern* EdgePattern::create(CypherAST* ast, Direction direction) {
    EdgePattern* pattern = new EdgePattern(direction);
    ast->addEntityPattern(pattern);
    return pattern;
}
