#include "MatchTargets.h"

#include "ASTContext.h"

using namespace db;

MatchTargets::MatchTargets()
{
}

MatchTargets::~MatchTargets() {
}

void MatchTargets::addTarget(MatchTarget* target) {
    _targets.push_back(target);
}

MatchTargets* MatchTargets::create(ASTContext* ctxt) {
    MatchTargets* targets = new MatchTargets();
    ctxt->addMatchTargets(targets);
    return targets;
}
