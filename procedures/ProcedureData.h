#pragma once

#include <stdint.h>
#include <stddef.h>
#include <type_traits>
#include <vector>

#include "columns/ColumnIndices.h"

namespace db {

class Column;

template <typename T>
class ColumnVector;

class ProcedureData {
public:
    ProcedureData();

    virtual ~ProcedureData();

    void resizeInputColumns(size_t size) {
        _inputColumns.resize(size);
    }

    void resizeReturnColumns(size_t size) {
        _returnColumns.resize(size);
    }

    const Column* getInputColumn(size_t i) {
        return _inputColumns[i];
    }

    Column* getReturnColumn(size_t i) {
        return _returnColumns[i];
    }

    void setInputColumn(size_t i, const Column* col) {
        _inputColumns[i] = col;
    }

    void setReturnColumn(size_t i, Column* col) {
        _returnColumns[i] = col;
    }

    // For each row the procedure emits, the index of the input row it derives from.
    //
    // A procedure need not emit one row per input row: it may emit several (expanding
    // one input into many) or none (dropping it). That leaves any column the query
    // carries past the call - the `n` of
    // `MATCH (n)-->(m) CALL f(m) YIELD x RETURN n, x` - misaligned with the emitted
    // rows, so the caller must replicate a carried row once per row the procedure
    // emitted for it and drop it where the procedure emitted none. This column is how
    // the procedure reports that correspondence: one index per emitted row, in emit
    // order, indexing the rows of its own input columns.
    //
    // The caller provides it only when it carries columns past the call, and clears it
    // before each execute; a procedure appends to it as it emits. It is null otherwise,
    // which a procedure must check - as it checks a return column it was not asked to
    // yield.
    //
    // Reporting is opt-in: a procedure that fills this declares it through
    // Procedure::setReportsInputRows, and a call that carries columns past one that does
    // not is refused while it is planned.
    ColumnVector<size_t>* getInputRowIndices() const {
        return _inputRowIndices;
    }

    void setInputRowIndices(ColumnVector<size_t>* indices) {
        _inputRowIndices = indices;
    }

private:
    std::vector<const Column*> _inputColumns;
    std::vector<Column*> _returnColumns;
    ColumnVector<size_t>* _inputRowIndices {nullptr};
};

class IndexedProcedureData : public ProcedureData {
public:
    void setIndices(ColumnIndices* indices) { _indices = indices; }

    ColumnIndices* indices() { return _indices; }

private:
    ColumnIndices* _indices {nullptr};
};

template <typename T>
concept ProcedureDataType = std::is_base_of_v<ProcedureData, T>;

}
