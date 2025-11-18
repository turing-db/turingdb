#include "ComputeExprProcessor.h"

#include "PipelineV2.h"
#include "ExprProgram.h"

using namespace db::v2;

ComputeExprProcessor::ComputeExprProcessor(ExprProgram* exprProg)
{
}

ComputeExprProcessor::~ComputeExprProcessor() {
}

ComputeExprProcessor* ComputeExprProcessor::create(Pipeline* pipeline,
                                                   ExprProgram* exprProg) {
    ComputeExprProcessor* computeExpr = new ComputeExprProcessor(exprProg);
    pipeline->addProcessor(computeExpr);
}

void ComputeExprProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void ComputeExprProcessor::reset() {
}

void ComputeExprProcessor::execute() {
    _input.getPort()->consume();
    _output.getPort()->writeData();

    _prog->execute();

    finish();
}
