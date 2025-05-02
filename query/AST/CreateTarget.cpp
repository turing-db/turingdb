#include "CreateTarget.h"

#include "ASTContext.h"

using namespace db;

CreateTarget* CreateTarget::create(ASTContext* ctxt, PathPattern* pattern) {
    auto* target = new CreateTarget(pattern);
    ctxt->addCreateTarget(target);
    return target;
}

CreateTargets* CreateTargets::create(ASTContext* ctxt) {
    CreateTargets* targets = new CreateTargets();
    ctxt->addCreateTargets(targets);
    return targets;
}
