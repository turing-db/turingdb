#include "EdgePattern.h"

#include "CypherAST.h"

using namespace db::v2;

EdgePattern::EdgePattern()
{
}

EdgePattern::~EdgePattern() {
}

EdgePattern* EdgePattern::create(CypherAST* ast) {
    EdgePattern* pattern = new EdgePattern();
    ast->addEntityPattern(pattern);
    return pattern;
}
