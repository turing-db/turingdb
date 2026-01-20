#include "ExprProgram.h"

#include <stdint.h>

#include "columns/ColumnIDs.h"
#include "columns/ColumnOperator.h"
#include "EvalBinaryExpr.h"
#include "EvalUnaryExpr.h"

#include "PipelineV2.h"

#include "FatalException.h"

using namespace db;

ExprProgram* ExprProgram::create(PipelineV2* pipeline) {
    ExprProgram* prog = new ExprProgram();
    pipeline->addExprProgram(prog);

    return prog;
}

void ExprProgram::evaluateInstructions() {
    for (const Instruction& instr : _instrs) {
        evalInstr(instr);
    }
}

void ExprProgram::evalInstr(const Instruction& instr) {
    const ColumnOperator op = instr._op;

    switch (getOperatorType(op)) {
        case ColumnOperatorType::OPTYPE_BINARY:
            evalBinaryInstr(instr);
        break;

        case ColumnOperatorType::OPTYPE_UNARY:
            evalUnaryInstr(instr);
        break;

        case ColumnOperatorType::OPTYPE_NOOP:
        break;
    }
}

void ExprProgram::evalBinaryInstr(const Instruction& instr) {
    const ColumnOperator op = instr._op;
    const Column* lhs = instr._lhs;
    const Column* rhs = instr._rhs;
    Column* res = instr._res;

    if (!lhs) {
        throw FatalException("Binary instruction had null left input.");
    }

    if (!rhs) {
        throw FatalException("Binary instruction had null right input.");
    }

    switch (op) {
        case OP_EQUAL: {
            EvalBinaryExpr::opEqual<OP_EQUAL>(res, lhs, rhs);
        } break;
        case OP_NOT_EQUAL: {
            EvalBinaryExpr::opEqual<OP_NOT_EQUAL>(res, lhs, rhs);
        } break;
        case OP_GREATER_THAN: {
            EvalBinaryExpr::opCompare<OP_GREATER_THAN>(res, lhs, rhs);
        } break;
        case OP_LESS_THAN: {
            EvalBinaryExpr::opCompare<OP_LESS_THAN>(res, lhs, rhs);
        } break;
        case OP_GREATER_THAN_OR_EQUAL: {
            EvalBinaryExpr::opCompareEqual<OP_GREATER_THAN_OR_EQUAL>(res, lhs, rhs);
        } break;
        case OP_LESS_THAN_OR_EQUAL: {
            EvalBinaryExpr::opCompareEqual<OP_LESS_THAN_OR_EQUAL>(res, lhs, rhs);
        } break;

        case OP_AND: {
            EvalBinaryExpr::opBoolean<OP_AND>(res, lhs, rhs);
        } break;
        case OP_OR: {
            EvalBinaryExpr::opBoolean<OP_OR>(res, lhs, rhs);
        } break;
        case OP_NOT: {
            throw FatalException("NOT binary operator is not supported.");
        } break;

        case OP_MINUS: {
            EvalBinaryExpr::opArithmetic<OP_MINUS>(res, lhs, rhs);
        } break;
        case OP_PLUS: {
            EvalBinaryExpr::opArithmetic<OP_PLUS>(res, lhs, rhs);
        } break;

        case OP_PROJECT: {
            throw FatalException("Project operator is not supported.");
        } break;
        case OP_IN: {
            throw FatalException("In operator is not supported.");
        } break;

        case OP_NOOP: {
            throw FatalException("NOOP operator is not supported.");
        } break;

        case _SIZE: {
            throw FatalException(
                "Attempted to evaluate invalid ColumnOperator.");
        } break;
    }
}

void ExprProgram::evalUnaryInstr(const Instruction& instr) {
    const ColumnOperator op = instr._op;
    const Column* input = instr._lhs;
    Column* res = instr._res;

    switch (op) {
        case OP_EQUAL: {
            throw FatalException("Equal operator is not supported.");
        } break;
        case OP_NOT_EQUAL: {
            throw FatalException("NotEqual operator is not supported.");
        } break;
        case OP_GREATER_THAN: {
            throw FatalException("GreaterThan operator is not supported.");
        } break;
        case OP_LESS_THAN: {
            throw FatalException("LessThan operator is not supported.");
        } break;
        case OP_GREATER_THAN_OR_EQUAL: {
            throw FatalException("GreaterThanOrEqual operator is not supported.");
        } break;
        case OP_LESS_THAN_OR_EQUAL: {
            throw FatalException("LessThanOrEqual operator is not supported.");
        } break;

        case OP_AND: {
            throw FatalException("And operator is not supported.");
        } break;
        case OP_OR: {
            throw FatalException("Or operator is not supported.");
        } break;
        case OP_NOT: {
            EvalUnaryExpr::opBoolean<OP_NOT>(res, input);
        } break;

        case OP_MINUS: {
            throw FatalException("Minus operator is not supported.");
        } break;
        case OP_PLUS: {
            throw FatalException("Plus operator is not supported.");
        } break;

        case OP_PROJECT: {
            throw FatalException("Project operator is not supported.");
        } break;
        case OP_IN: {
            throw FatalException("In operator is not supported.");
        } break;

        case OP_NOOP: {
            throw FatalException("NOOP operator is not supported.");
        } break;

        case _SIZE: {
            throw FatalException(
                "Attempted to evaluate invalid ColumnOperator.");
        } break;
    }
}
