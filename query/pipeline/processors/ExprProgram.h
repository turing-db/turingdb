#pragma once

#include <vector>

#include "columns/ColumnOperator.h"

#include "list/ListBuffer.h"
#include "views/GraphView.h"

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

    void setView(GraphView view) { _view = view; }

    // The query's list buffer, which labels() builds its rows in. Set by the generator
    // when it emits an instruction needing one, so a program without such an instruction
    // holds none.
    void setListBuffer(QueryListBuffer* listBuffer) { _listBuffer = listBuffer; }

private:
    // All instructions which need be evaluated
    Instructions _instrs;
    GraphView _view;
    QueryListBuffer* _listBuffer {nullptr};

    ExprProgram() = default;
    virtual ~ExprProgram() = default;
    void evalInstr(const Instruction& instr);
    void evalBinaryInstr(const Instruction& instr);
    void evalUnaryInstr(const Instruction& instr);
    void evalFunction(const Instruction& instr);
};

}
