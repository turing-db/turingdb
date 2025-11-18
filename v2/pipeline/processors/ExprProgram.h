#pragma once

#include <vector>

#include "columns/ColumnOperator.h"

namespace db {
class Column;
}

namespace db::v2 {

class EvalProgram {
public:
    struct Instruction {
        ColumnOperator _op;
        Column* _res {nullptr};
        Column* _lhs {nullptr};
        Column* _rhs {nullptr};
    };

    using Instructions = std::vector<Instruction>;

    EvalProgram();
    ~EvalProgram();

    const Instructions& instrs() const { return _instrs; }

    void addInstr(const Instruction& instr) {
        _instrs.emplace_back(instr);
    }

    void execute();

private:
    Instructions _instrs;

    void evalInstr(const Instruction& instr);
};

}
