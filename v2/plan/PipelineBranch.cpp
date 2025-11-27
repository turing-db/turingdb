#include "PipelineBranch.h"

#include "PipelineBranches.h"

using namespace db::v2;

PipelineBranch::PipelineBranch()
{
}

PipelineBranch::~PipelineBranch() {
}

PipelineBranch* PipelineBranch::create(PipelineBranches* branches) {
    PipelineBranch* branch = new PipelineBranch();
    branches->addBranch(branch);
    return branch;
}
