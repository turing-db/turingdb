#include "PlanBranch.h"

#include "PlanBranches.h"

using namespace db::v2;

PlanBranch::PlanBranch()
{
}

PlanBranch::~PlanBranch() {
}

PlanBranch* PlanBranch::create(PlanBranches* branches) {
    PlanBranch* branch = new PlanBranch();
    branches->addBranch(branch);
    return branch;
}
