#pragma once

#include <stddef.h>
#include <vector>

#include "NLStepBuffers.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

#include "NLFunctionData.h"
#include "NLColumnFunctions.h"

namespace db {

// Lay a constant column's single value out over rowCount rows of a fresh output
// chunk, as one present value per row. The cross product's broadcast repeats a
// chunk's several values; this one repeats the single value a ColumnConst holds,
// so a fold that walks rows sees the step's rows rather than the one row the
// constant is. One per value type, selected during translation.
using NLBroadcastConstantFunction = void (*)(const Column* value,
                                             size_t rowCount,
                                             Column* output);

// nl.broadcast_constant data: the constant column to repeat, the driving
// relation's chunk whose row count says how many times - null when no relation
// drives the projection, which is a single row - the output chunk to fill and the
// fill that writes it.
class NLBroadcastConstantData : public NLFunctionData {
public:
    NLBroadcastConstantData(const Column* value,
                            const Column* cardinality,
                            Column* output,
                            NLBroadcastConstantFunction fill)
        : _value(value),
        _cardinality(cardinality),
        _output(output),
        _fill(fill)
    {
    }

    const Column* getValue() const { return _value; }
    const Column* getCardinality() const { return _cardinality; }
    Column* getOutput() const { return _output; }
    NLBroadcastConstantFunction getFill() const { return _fill; }

private:
    const Column* _value {nullptr};
    const Column* _cardinality {nullptr};
    Column* _output {nullptr};
    NLBroadcastConstantFunction _fill {nullptr};
};

// Binary function to execute
using NLBinaryFn = void (*)(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* stepBuffers);

class NLBinaryData : public NLFunctionData {
public:
    NLBinaryData(const Column* lhs, const Column* rhs, Column* result, NLBinaryFn fn, NLStepBuffers* stepBuffers)
        : _lhs(lhs),
        _rhs(rhs),
        _result(result),
        _fn(fn),
        _stepBuffers(stepBuffers)
    {
    }

    const Column* getLhs() const { return _lhs; }
    const Column* getRhs() const { return _rhs; }
    Column* getResult() const { return _result; }
    NLBinaryFn getFn() const { return _fn; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

private:
    const Column* _lhs {nullptr};
    const Column* _rhs {nullptr};
    Column* _result {nullptr};
    NLBinaryFn _fn {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
};

// Unary function to execute
using NLUnaryFn = void (*)(Column* result, const Column* operand);

// Unary function to execute, plus operand and result
class NLUnaryData : public NLFunctionData {
public:
    NLUnaryData(const Column* operand, Column* result, NLUnaryFn fn)
        : _operand(operand),
        _result(result),
        _fn(fn)
    {
    }

    const Column* getOperand() const { return _operand; }
    Column* getResult() const { return _result; }
    NLUnaryFn getFn() const { return _fn; }

private:
    const Column* _operand {nullptr};
    Column* _result {nullptr};
    NLUnaryFn _fn {nullptr};
};

// Whether row @param row of a CASE branch's condition column holds true. A null
// condition never does, so its rows fall through to the next branch.
using NLCaseTestFn = bool (*)(const Column* condition, size_t row);

// Copies row @param valueRow of a CASE branch's value column into row @param resultRow of
// the result. nl.case reads its values at the row it writes; nl.case_merge reads each at
// the row its cursor reached, since a value holds only the rows that took its branch.
using NLCaseWriteFn = void (*)(Column* result, size_t resultRow, const Column* value, size_t valueRow);

// Sizes a CASE result to @param rowCount rows, all absent: the value a row unmatched by
// every branch of a defaultless CASE holds
using NLCaseResetFn = void (*)(Column* result, size_t rowCount);

// Ends the row writes into a CASE result: a nullable result whose rows all took a value
// drops the validity mask its reset built. Null for a result that has no mask.
using NLCaseFinishFn = void (*)(Column* result);

// How a value is written into one row of a CASE result or an nl.reduce accumulator:
// through a CASE write where the result is a typed column, or read as a list item and
// stored as a tagged cell where it is a !storage.any column
struct NLCellWrite {
    NLCaseWriteFn _write {nullptr};
    NLListItemReadFunction _taggedRead {nullptr};
};

// One CASE, as the rows are walked: per branch the condition to read, the value to take
// when it holds, and the kernels that read each. The default is what a row matching no
// branch takes; with none, the reset already left it absent.
class NLCaseData : public NLFunctionData {
public:
    struct Branch {
        const Column* _condition {nullptr};
        const Column* _value {nullptr};
        NLCaseTestFn _test {nullptr};
        NLCellWrite _write;
    };

    struct BranchCursor {
        size_t _conditionRow {0};
        size_t _valueRow {0};
    };

    NLCaseData(const Column* cardinality,
               Column* result,
               NLCaseResetFn reset,
               NLCaseFinishFn finish,
               NLStepBuffers* stepBuffers)
        : _cardinality(cardinality),
        _result(result),
        _reset(reset),
        _finish(finish),
        _stepBuffers(stepBuffers)
    {
    }

    const Column* getCardinality() const { return _cardinality; }
    Column* getResult() const { return _result; }
    NLCaseResetFn getReset() const { return _reset; }
    NLCaseFinishFn getFinish() const { return _finish; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

    const Column* getDefaultValue() const { return _defaultValue; }
    const NLCellWrite& getWriteDefault() const { return _writeDefault; }

    const std::vector<Branch>& branches() const { return _branches; }
    std::vector<BranchCursor>& cursors() { return _cursors; }

    void addBranch(const Branch& branch) {
        _branches.push_back(branch);
        _cursors.emplace_back();
    }

    void setDefault(const Column* defaultValue, const NLCellWrite& writeDefault) {
        _defaultValue = defaultValue;
        _writeDefault = writeDefault;
    }

private:
    const Column* _cardinality {nullptr};
    Column* _result {nullptr};
    NLCaseResetFn _reset {nullptr};
    NLCaseFinishFn _finish {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
    std::vector<Branch> _branches;
    std::vector<BranchCursor> _cursors;
    const Column* _defaultValue {nullptr};
    NLCellWrite _writeDefault;
};

// Each row @param nullRows marks takes the next row of @param fill, every other row its own
using NLFillNullRowsFn = void (*)(Column* result, const Column* value, const ColumnVector<CustomBool>* nullRows, const Column* fill);

class NLFillNullsData : public NLFunctionData {
public:
    NLFillNullsData(const Column* value,
                    const ColumnVector<CustomBool>* nullRows,
                    const Column* fill,
                    Column* result,
                    NLFillNullRowsFn fillNullRows);
    ~NLFillNullsData();

    const Column* getValue() const { return _value; }
    const ColumnVector<CustomBool>* getNullRows() const { return _nullRows; }
    const Column* getFill() const { return _fill; }
    Column* getResult() const { return _result; }
    NLFillNullRowsFn getFillNullRows() const { return _fillNullRows; }

private:
    const Column* _value {nullptr};
    const ColumnVector<CustomBool>* _nullRows {nullptr};
    const Column* _fill {nullptr};
    Column* _result {nullptr};
    NLFillNullRowsFn _fillNullRows {nullptr};
};

// One nl.to_any: the cells of the operand read as list items, and stored as tagged cells
// in the values of the run that read them
class NLToAnyData : public NLFunctionData {
public:
    NLToAnyData(const Column* operand, Column* result, NLListItemReadFunction read, NLStepBuffers* stepBuffers)
        : _operand(operand),
        _result(result),
        _read(read),
        _stepBuffers(stepBuffers)
    {
    }

    const Column* getOperand() const { return _operand; }
    Column* getResult() const { return _result; }
    NLListItemReadFunction getRead() const { return _read; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

private:
    const Column* _operand {nullptr};
    Column* _result {nullptr};
    NLListItemReadFunction _read {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
};

// nl.to_nullable data for the null literal's chunk: the result holds one absent value per
// row of the chunk the null was laid out over, in the type the result's own chunk names
class NLFillNullData : public NLFunctionData {
public:
    NLFillNullData(const Column* rows, Column* output, NLFillNullFunction fill)
        : _rows(rows),
        _output(output),
        _fill(fill)
    {
    }

    const Column* getRows() const { return _rows; }
    Column* getOutput() const { return _output; }
    NLFillNullFunction getFill() const { return _fill; }

private:
    const Column* _rows {nullptr};
    Column* _output {nullptr};
    NLFillNullFunction _fill {nullptr};
};

}
