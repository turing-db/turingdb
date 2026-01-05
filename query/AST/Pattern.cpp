#include "Pattern.h"

#include "CypherAST.h"

using namespace db::v2;

Pattern::Pattern()
{
}

Pattern::~Pattern() {
}

Pattern* Pattern::create(CypherAST* ast) {
    Pattern* pattern = new Pattern();
    ast->addPattern(pattern);
    return pattern;
}
