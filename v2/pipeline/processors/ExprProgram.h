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

    void addTopLevelResult(Column* resultCol) { _topLevelResults.push_back(resultCol); }
    const std::vector<Column*>& getTopLevelResults() const { return _topLevelResults; }

    void evaluateInstructions();

private:
    Instructions _instrs;
    std::vector<Column*> _topLevelResults;

    ExprProgram() = default;
    ~ExprProgram() = default;
    void evalInstr(const Instruction& instr);
    void evalBinaryInstr(const Instruction& instr);
    void evalUnaryInstr(const Instruction& instr);
};

}
