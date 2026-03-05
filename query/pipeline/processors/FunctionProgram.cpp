#include "FunctionProgram.h"

#include "PipelineV2.h"

#include "EvalFunction.h"
#include "columns/ColumnOperator.h"

using namespace db;

FunctionProgram* FunctionProgram::create(PipelineV2* pipeline) {
    FunctionProgram* prog = new FunctionProgram();    
    pipeline->addExprProgram(prog);

    return prog;
}

void FunctionProgram::evaluateInstructions() {
    for (const Instruction& instr : _instrs) {
        evalInstr(instr);
    }
}

void FunctionProgram::evalInstr(const Instruction& instr) {
    const ColumnOperator op = instr._op;

    switch (getOperatorType(op)) {
        case ColumnOperatorType::OPTYPE_BINARY:
            throw FatalException("Attempted to evaluate binary operator as function.");
        break;

        case ColumnOperatorType::OPTYPE_UNARY:
            throw FatalException("Attempted to evaluate unary operator as function.");
        break;

        case ColumnOperatorType::OPTYPE_FUNC:
            evalFunction(instr);
        break;

        case ColumnOperatorType::OPTYPE_NOOP:
        break;
    }
}

void FunctionProgram::evalFunction(const Instruction& instr) {
    const ColumnOperator op = instr._op;
    const Column* arg = instr._lhs;
    Column* res = instr._res;

    switch (op) {
        case OP_FUNC_LABELS:
            EvalFunction::eval<OP_FUNC_LABELS>(res, arg, _view);
        break;

        case OP_EQUAL:
        case OP_NOT_EQUAL:
        case OP_GREATER_THAN:
        case OP_LESS_THAN:
        case OP_GREATER_THAN_OR_EQUAL:
        case OP_LESS_THAN_OR_EQUAL:
        case OP_AND:
        case OP_OR:
        case OP_ADD:
        case OP_SUB:
        case OP_MUL:
        case OP_DIV:
        case OP_PROJECT:
        case OP_IN:
        case OP_MINUS:
        case OP_PLUS:
        case OP_NOT:
        case OP_TO_INTEGER:
        case OP_TO_FLOAT:
        case OP_TO_BOOLEAN:
            throw FatalException(fmt::format("Attempted to evalute {} as function.",
                                             ColumnOperatorDescription::value(op)));
        break;

        case OP_NOOP:
        case _SIZE:
            throw FatalException("Attempted to evaluate invalid ColumnOperator.");
        break;
    }
}
