#include "PredicateProgram.h"
#include "PipelineV2.h"

using namespace db;

PredicateProgram* PredicateProgram::create(PipelineV2* pipeline) {
    auto* prog = new PredicateProgram();
    pipeline->addExprProgram(prog);

    return prog;
}
