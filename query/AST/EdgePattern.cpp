#include "EdgePattern.h"

#include "CypherAST.h"

using namespace db;

EdgePattern::EdgePattern(Direction direction)
    : _direction(direction)
{
}

EdgePattern::~EdgePattern() {
}

EdgePattern* EdgePattern::create(CypherAST* ast, QuantifiedPath* quantified, Direction direction) {
    EdgePattern* pattern = new EdgePattern(direction);
    pattern->setQuantifiedPath(quantified);
    ast->addEntityPattern(pattern);
    return pattern;
}

void EdgePattern::addHop(EdgePattern* hop) {
    const Direction hopDirection = hop->getDirection();

    if (_hops.empty()) {
        _direction = hopDirection;
    } else if (hopDirection != _direction) {
        _direction = Direction::Undirected;
    }

    _hops.push_back(hop);
}
