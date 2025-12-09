#include "ComputeExprProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "PipelineV2.h"
#include "ExprProgram.h"
#include "spdlog/spdlog.h"

using namespace db::v2;

ComputeExprProcessor::ComputeExprProcessor(ExprProgram* exprProg)
    : _exprProg(exprProg)
{
}

std::string ComputeExprProcessor::describe() const {
    return fmt::format("ComputeExprProcessor @={}", fmt::ptr(this));
}

ComputeExprProcessor* ComputeExprProcessor::create(PipelineV2* pipeline,
                                                   ExprProgram* exprProg) {
    ComputeExprProcessor* computeExpr = new ComputeExprProcessor(exprProg);

    {
        PipelineInputPort* input = PipelineInputPort::create(pipeline, computeExpr);
        computeExpr->_input.setPort(input);
        computeExpr->addInput(input);
    }

    {
        PipelineOutputPort* output = PipelineOutputPort::create(pipeline, computeExpr);
        computeExpr->_output.setPort(output);
        computeExpr->addOutput(output);
    }

    computeExpr->postCreate(pipeline);

    return computeExpr;
}

void ComputeExprProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void ComputeExprProcessor::reset() {
    markAsReset();
}

void ComputeExprProcessor::execute() {
    spdlog::info("Expr::execute");
    _input.getPort()->consume();
    _output.getPort()->writeData();

    _exprProg->evaluateInstructions();

    finish();
}
