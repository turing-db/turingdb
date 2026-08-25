#include "ExprProgram.h"

#include <spdlog/fmt/fmt.h>

#include "EvalBinaryExpr.h"
#include "EvalFunction.h"
#include "EvalUnaryExpr.h"

#include "columns/ColumnOperator.h"

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

        case ColumnOperatorType::OPTYPE_FUNC:
            evalFunction(instr);
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
        case OP_EQUAL:
            EvalBinaryExpr::eval<OP_EQUAL>(res, lhs, rhs);
        break;

        case OP_NOT_EQUAL:
            EvalBinaryExpr::eval<OP_NOT_EQUAL>(res, lhs, rhs);
        break;

        case OP_GREATER_THAN:
            EvalBinaryExpr::eval<OP_GREATER_THAN>(res, lhs, rhs);
        break;

        case OP_LESS_THAN:
            EvalBinaryExpr::eval<OP_LESS_THAN>(res, lhs, rhs);
        break;

        case OP_GREATER_THAN_OR_EQUAL:
            EvalBinaryExpr::eval<OP_GREATER_THAN_OR_EQUAL>(res, lhs, rhs);
        break;

        case OP_LESS_THAN_OR_EQUAL:
            EvalBinaryExpr::eval<OP_LESS_THAN_OR_EQUAL>(res, lhs, rhs);
        break;

        case OP_AND:
            EvalBinaryExpr::eval<OP_AND>(res, lhs, rhs);
        break;

        case OP_OR:
            EvalBinaryExpr::eval<OP_OR>(res, lhs, rhs);
        break;

        case OP_ADD:
            EvalBinaryExpr::eval<OP_ADD>(res, lhs, rhs);
        break;

        case OP_SUB:
            EvalBinaryExpr::eval<OP_SUB>(res, lhs, rhs);
        break;

        case OP_MUL:
            EvalBinaryExpr::eval<OP_MUL>(res, lhs, rhs);
        break;

        case OP_DIV:
            EvalBinaryExpr::eval<OP_DIV>(res, lhs, rhs);
        break;

        case OP_XOR:
            EvalBinaryExpr::eval<OP_XOR>(res, lhs, rhs);
        break;

        case OP_MINUS:
        case OP_PLUS:
        case OP_NOT:
        case OP_TO_INTEGER:
        case OP_TO_FLOAT:
        case OP_TO_BOOLEAN:
        case OP_FUNC_LABELS:
        case OP_FUNC_EDGE_TYPES:
        case OP_FUNC_COSINE_SIMILARITY:
        case OP_FUNC_EUCLIDEAN_DISTANCE:
            throw FatalException(
                fmt::format("Attempted to evaluate {} as binary operator.",
                            ColumnOperatorDescription::value(op)));
        break;

        case OP_CONCAT:
        case OP_MOD:
        case OP_POW:
        case OP_STARTS_WITH:
        case OP_ENDS_WITH:
        case OP_CONTAINS:
        case OP_PROJECT:
        case OP_IN:
            throw FatalException(
                fmt::format("Unsupported binary operator: {}.",
                            ColumnOperatorDescription::value(op)));
        break;

        case OP_NOOP:
        case _SIZE:
            throw FatalException("Attempted to evaluate invalid ColumnOperator.");
        break;
    }
}

void ExprProgram::evalUnaryInstr(const Instruction& instr) {
    const ColumnOperator op = instr._op;
    const Column* input = instr._lhs;
    Column* res = instr._res;

    switch (op) {
        case OP_NOT:
            EvalUnaryExpr::eval<OP_NOT>(res, input);
        break;

        case OP_MINUS:
            throw FatalException("Minus operator is not supported.");
        break;

        case OP_PLUS:
            throw FatalException("Plus operator is not supported.");
        break;

        case OP_TO_INTEGER:
        case OP_TO_FLOAT:
        case OP_TO_BOOLEAN:
        case OP_FUNC_LABELS:
        case OP_FUNC_EDGE_TYPES:
        case OP_FUNC_COSINE_SIMILARITY:
        case OP_FUNC_EUCLIDEAN_DISTANCE:

        case OP_EQUAL:
        case OP_NOT_EQUAL:
        case OP_GREATER_THAN:
        case OP_LESS_THAN:
        case OP_GREATER_THAN_OR_EQUAL:
        case OP_LESS_THAN_OR_EQUAL:
        case OP_AND:
        case OP_OR:
        case OP_ADD:
        case OP_CONCAT:
        case OP_SUB:
        case OP_MUL:
        case OP_DIV:
        case OP_MOD:
        case OP_POW:
        case OP_STARTS_WITH:
        case OP_ENDS_WITH:
        case OP_CONTAINS:
        case OP_PROJECT:
        case OP_IN:
        case OP_XOR:
            throw FatalException(fmt::format(
                "Attempted to evalute {} as unary operator.",
                ColumnOperatorDescription::value(op)));
        break;

        case OP_NOOP:
        case _SIZE:
            throw FatalException(
                "Attempted to evaluate invalid ColumnOperator.");
        break;
    }
}

void ExprProgram::evalFunction(const Instruction& instr) {
    const ColumnOperator op = instr._op;
    const Column* arg = instr._lhs;
    Column* res = instr._res;

    switch (op) {
        case OP_TO_INTEGER:
            EvalFunction::eval<OP_TO_INTEGER>(res, arg);
        break;

        case OP_TO_FLOAT:
            EvalFunction::eval<OP_TO_FLOAT>(res, arg);
        break;

        case OP_TO_BOOLEAN:
            EvalFunction::eval<OP_TO_BOOLEAN>(res, arg);
        break;

        case OP_FUNC_LABELS:
            EvalFunction::eval<OP_FUNC_LABELS>(res, arg, _view);
        break;

        case OP_FUNC_EDGE_TYPES:
            EvalFunction::eval<OP_FUNC_EDGE_TYPES>(res, arg, _view);
        break;

        case OP_FUNC_COSINE_SIMILARITY:
            EvalFunction::eval<OP_FUNC_COSINE_SIMILARITY>(res, instr._lhs, instr._rhs);
        break;

        case OP_FUNC_EUCLIDEAN_DISTANCE:
            EvalFunction::eval<OP_FUNC_EUCLIDEAN_DISTANCE>(res, instr._lhs, instr._rhs);
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
        case OP_CONCAT:
        case OP_SUB:
        case OP_MUL:
        case OP_DIV:
        case OP_MOD:
        case OP_POW:
        case OP_STARTS_WITH:
        case OP_ENDS_WITH:
        case OP_CONTAINS:
        case OP_PROJECT:
        case OP_IN:
        case OP_MINUS:
        case OP_PLUS:
        case OP_NOT:
        case OP_XOR:
            throw FatalException(fmt::format("Attempted to evalute {} as function.",
                                             ColumnOperatorDescription::value(op)));
        break;

        case OP_NOOP:
        case _SIZE:
            throw FatalException("Attempted to evaluate invalid ColumnOperator.");
        break;
    }
}
