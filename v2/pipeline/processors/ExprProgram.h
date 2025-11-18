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

    struct Instruction {
        ColumnOperator _op;
        Column* _res {nullptr};
        Column* _lhs {nullptr};
        Column* _rhs {nullptr};
    };

    using Instructions = std::vector<Instruction>;

    static ExprProgram* create(PipelineV2* pipeline);

    const Instructions& instrs() const { return _instrs; }

    void addInstr(const Instruction& instr) {
        _instrs.emplace_back(instr);
    }

    void execute();

private:
    Instructions _instrs;

    ExprProgram();
    ~ExprProgram();
    void evalInstr(const Instruction& instr);
};

}
