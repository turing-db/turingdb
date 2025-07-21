#pragma once

#include <vector>
#include <string>

#include "columns/ColumnVector.h"
#include "columns/ColumnOperator.h"

namespace db {

class ExecutionContext;
class ColumnMask;

class FilterStep {
public:
    struct Tag {};

    // Expression to compute the filter mask
    struct Expression {
        ColumnOperator _op {};
        ColumnMask* _mask {nullptr};
        const Column* _lhs {nullptr};
        const Column* _rhs {nullptr};
    };

    // Operand columns on which a mask will be applied
    // _src: column on which the mask will be applied
    // _dest: column written as a result of mask application
    struct Operand {
        const ColumnMask* _mask {nullptr};
        const Column* _src {nullptr};
        Column* _dest {nullptr};
    };

    FilterStep(ColumnVector<size_t>* indices);
    FilterStep();
    FilterStep(FilterStep&&) = default;
    ~FilterStep();

    FilterStep(const FilterStep&) = delete;
    FilterStep& operator=(const FilterStep&) = delete;
    FilterStep& operator=(FilterStep&&) = delete;

    void addExpression(Expression&& expression);
    void addOperand(Operand&& operand);

    void prepare(ExecutionContext* ctxt) {}

    void reset();

    inline bool isFinished() const { return true; }

    void execute();

    void describe(std::string& descr) const;

private:
    ColumnVector<size_t>* _indices {nullptr};
    std::vector<Expression> _expressions;
    std::vector<Operand> _operands;

    void compute();
    void generateIndices();
};

}
