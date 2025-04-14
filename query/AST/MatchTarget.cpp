#include "MatchTarget.h"

#include "ASTContext.h"

using namespace db;

MatchTarget::MatchTarget(PathPattern* pattern)
    : _pattern(pattern)
{
}

MatchTarget::~MatchTarget() {
}

MatchTarget* MatchTarget::create(ASTContext* ctxt, PathPattern* pattern) {
    MatchTarget* target = new MatchTarget(pattern);
    ctxt->addMatchTarget(target);
    return target;
}
