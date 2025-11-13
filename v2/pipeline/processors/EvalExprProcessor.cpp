#include "EvalExprProcessor.h"

#include "EvalProgram.h"

using namespace db::v2;

EvalExprProcessor::EvalExprProcessor()
    : _prog(std::make_unique<EvalProgram>())
{
}

EvalExprProcessor::~EvalExprProcessor() {
}

void EvalExprProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void EvalExprProcessor::reset() {
}

void EvalExprProcessor::execute() {
    _input.getPort()->consume();
    _output.getPort()->writeData();

    _prog->execute();

    finish();
}
