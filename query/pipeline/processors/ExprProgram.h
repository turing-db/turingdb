#pragma once

#include <vector>

#include "columns/ColumnOperator.h"

namespace db {
class Column;
}

namespace db::v2 {

class PipelineV2;

class ExprProgram {
public:
    friend PipelineV2;
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

    void addTopLevelPredicate(Column* resultCol) { _topLevelPredicate.push_back(resultCol); }
    const std::vector<Column*>& getTopLevelPredicates() const { return _topLevelPredicate; }

    void evaluateInstructions();

private:
    // All instructions which need be evaluated
    Instructions _instrs;
    // Contains pointers to the result columns of different instructions. All need be true
    // TODO: Refactor this into derived class just for filters
    std::vector<Column*> _topLevelPredicate;

    ExprProgram() = default;
    ~ExprProgram() = default;
    void evalInstr(const Instruction& instr);
    void evalBinaryInstr(const Instruction& instr);
    void evalUnaryInstr(const Instruction& instr);
};

}
