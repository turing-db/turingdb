#include "ExprProgram.h"

#include <stdint.h>

#include "columns/ColumnOperator.h"
#include "columns/ColumnOptVector.h"
#include "EvalBinaryExpr.h"
#include "EvalUnaryExpr.h"
#include "metadata/SupportedType.h"

#include "PipelineV2.h"

#include "FatalException.h"
#include "PipelineException.h"

#include <spdlog/fmt/fmt.h>

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

        case OP_MINUS:
        case OP_PLUS:
        case OP_NOT:
        case OP_TO_INTEGER:
        case OP_TO_FLOAT:
        case OP_TO_BOOLEAN:
            throw FatalException(
                fmt::format("Attempted to evaluate {} as binary operator.",
                            ColumnOperatorDescription::value(op)));
        break;

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

        case OP_TO_INTEGER: {
            using StringCol = ColumnVector<std::string>;
            using IntCol = ColumnOptVector<types::Int64::Primitive>;
            const auto* src = static_cast<const StringCol*>(input);
            auto* dst = static_cast<IntCol*>(res);
            dst->resize(src->size());
            for (size_t i = 0; i < src->size(); i++) {
                const auto& val = (*src)[i];
                try {
                    (*dst)[i] = std::stoll(val);
                } catch (...) {
                    throw PipelineException(
                        fmt::format("toInteger: cannot convert '{}' to integer", val));
                }
            }
        } break;

        case OP_TO_FLOAT: {
            using StringCol = ColumnVector<std::string>;
            using DblCol = ColumnOptVector<types::Double::Primitive>;
            const auto* src = static_cast<const StringCol*>(input);
            auto* dst = static_cast<DblCol*>(res);
            dst->resize(src->size());
            for (size_t i = 0; i < src->size(); i++) {
                const auto& val = (*src)[i];
                try {
                    (*dst)[i] = std::stod(val);
                } catch (...) {
                    throw PipelineException(
                        fmt::format("toFloat: cannot convert '{}' to float", val));
                }
            }
        } break;

        case OP_TO_BOOLEAN: {
            using StringCol = ColumnVector<std::string>;
            using BoolCol = ColumnOptVector<types::Bool::Primitive>;
            const auto* src = static_cast<const StringCol*>(input);
            auto* dst = static_cast<BoolCol*>(res);
            dst->resize(src->size());
            std::string lower;
            for (size_t i = 0; i < src->size(); i++) {
                const auto& val = (*src)[i];
                lower.clear();
                lower.reserve(val.size());
                for (char c : val) {
                    lower += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
                }
                if (lower == "true") {
                    (*dst)[i] = CustomBool(true);
                } else if (lower == "false") {
                    (*dst)[i] = CustomBool(false);
                } else {
                    throw PipelineException(
                        fmt::format("toBoolean: cannot convert '{}' to boolean", val));
                }
            }
        } break;

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
