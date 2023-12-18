#include "FromTarget.h"

#include "ASTContext.h"

using namespace db;

FromTarget::FromTarget(PathPattern* pattern)
    : _pattern(pattern)
{
}

FromTarget::~FromTarget() {
}

FromTarget* FromTarget::create(ASTContext* ctxt, PathPattern* pattern) {
    FromTarget* target = new FromTarget(pattern);
    ctxt->addFromTarget(target);
    return target;
}
