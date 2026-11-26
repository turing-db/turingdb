#pragma once

#include <stddef.h>
#include <stdint.h>
#include <utility>
#include <vector>

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"

namespace mlir::db {

using ColumnEquality = std::pair<Value, Value>;

// Every equality of two node columns the filter requires: it keeps only the rows where they
// agree, so below it anything reading one of them reads the other just as well. A filter
// whose mask is a conjunction requires each of its conjuncts, equalities among them.
void collectColumnEqualities(FilterOp filter, llvm::SmallVectorImpl<ColumnEquality>& equalities);

// One step of the climb from a column to the column its variable was bound at. It crosses a
// row op where the op is not a filter: it builds, orders or groups the rows rather than only
// dropping some
struct LineageStep {
    Value _next;
    bool _isAnchor {false};
    bool _crossesRowOp {false};
};

// Sets _isAnchor where @param column is where its variable was bound: a node source, or an op
// carrying the rows in flight that makes the column. Sets _next to the column the climb goes
// on from, and neither where the climb cannot pass the op defining it
void stepTowardLineageAnchor(Value column, LineageStep& step);

// The climbs of one pass run, each built from the climb one step up. A push_down_filters push
// moves a predicate onto the op making its anchors, which changes no anchor and crosses no row
// op, so a climb stays true across pushes; its equalities are kept as their sides' anchors,
// which a push never erases. A rewrite that replaces an anchor has to clear() them.
class LineageClimbs {
public:
    Value anchorOf(Value column);

    Value climb(Value column, bool& crossedRowOp, llvm::SmallVectorImpl<ColumnEquality>& anchorEqualities);

    void forget(Operation* op);

    void clear();

private:
    static constexpr size_t NO_LIST = SIZE_MAX;

    struct Climb {
        Value _anchor;
        bool _crossedRowOp {false};
        size_t _equalities {NO_LIST};
    };

    // A climb's distinct equalities, nearest filter first. A filter whose equalities the climb
    // one step up already holds shares that list, so a chain repeating one equality keeps one.
    llvm::DenseMap<Value, Climb> _climbs;
    std::vector<llvm::SmallVector<ColumnEquality, 2>> _equalityLists;

    void computeClimb(Value column);
};

}
