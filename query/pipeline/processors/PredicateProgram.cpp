#include "PredicateProgram.h"

#include "PipelineV2.h"

using namespace db;

PredicateProgram* PredicateProgram::create(PipelineV2* pipeline, StringBuffer* stringBuffer) {
    PredicateProgram* prog = new PredicateProgram();
    prog->_stringBuffer = stringBuffer;
    pipeline->addExprProgram(prog);

    return prog;
}
