#pragma once

#include <vector>

#include "columns/ColumnOperator.h"

namespace db {

class PipelineV2;
class Column;
class PredicateProgram;

class ExprProgram {
public:
    friend PipelineV2;
    friend PredicateProgram;
    struct Instruction;

    using Instructions = std::vector<Instruction>;

    struct Instruction {
        ColumnOperator _op {ColumnOperator::_SIZE};
        Column* _res {nullptr};
        Column* _lhs {nullptr};
        Column* _rhs {nullptr};
    };

    static ExprProgram* create(PipelineV2* pipeline);

    const Instructions& instrs() const { return _instrs; }

    template <typename... Args>
    void addInstr(Args&&... args) {
        _instrs.emplace_back(std::forward<Args>(args)...);
    }

    void evaluateInstructions();

private:
    // All instructions which need be evaluated
    Instructions _instrs;

    ExprProgram() = default;
    virtual ~ExprProgram() = default;
    void evalInstr(const Instruction& instr);
    void evalBinaryInstr(const Instruction& instr);
    void evalUnaryInstr(const Instruction& instr);
};

}
