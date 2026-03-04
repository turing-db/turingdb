#include "FunctionProgram.h"

#include "PipelineV2.h"

using namespace db;

FunctionProgram* FunctionProgram::create(PipelineV2* pipeline) {
    FunctionProgram* prog = new FunctionProgram();    
    pipeline->addExprProgram(prog);

    return prog;
}
