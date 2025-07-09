#pragma once

#include <vector>

namespace db {

class MatchTarget;
class ASTContext;

class MatchTargets {
public:
    friend ASTContext;
    using Targets = std::vector<MatchTarget*>;

    const Targets& targets() const { return _targets; }

    void addTarget(MatchTarget* target);

    static MatchTargets* create(ASTContext* ctxt);

private:
    Targets _targets;

    MatchTargets();
    ~MatchTargets();
};

}
