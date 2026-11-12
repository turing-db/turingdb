#pragma once

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/STLFunctionalExtras.h"
#include "llvm/ADT/SmallVector.h"

namespace mlir::db {

// The column each row of @param column holds the value of, where the op it comes out of only
// carries it: the row source of a result, or the operand a region's argument is carried in
// from. Null where that op makes the column.
Value rowSourceOf(Value column);

// Whether a climb goes on from @param column to its row source
using ClimbsFrom = llvm::function_ref<bool(Value)>;

// The row source of @param column where @param climbsFrom lets a climb go on from it, else null
Value stepToRowSource(Value column, ClimbsFrom climbsFrom);

// The column @param column holds the values of, climbing from each column to its row source
// for as long as @param climbsFrom lets it
Value climbRowSource(Value column, ClimbsFrom climbsFrom);

// Calls @param visit on each value @param op and the ops nested in it define
void visitDefinedValues(Operation* op, llvm::function_ref<void(Value)> visit);

// A fold over the steps of a climb, each from a column to its row source. Fold::Summary is what
// it keeps of a run of steps, Summary {} the run of none, step(column) the run of the one step
// from column and combine(near, far) the run of near then far, which is associative. This fold
// keeps nothing, so its climbs answer only where they end.
struct NoStepSummary {
    struct Summary {
    };

    Summary step(Value column) const { return {}; }
    Summary combine(Summary near, Summary far) const { return {}; }
    void clear() {}
};

// The climbs of climbRowSource over one predicate, which outlives them, each with what Fold
// keeps of its steps. A column climbs once and hands its answer and summary to every column
// climbing through it, so the columns of a chain of N ops climb N steps in all. A rewrite of the
// ops climbed through clear()s the answers, or forget()s an op it erases if it changes no other.
template <typename Fold>
class FoldedRowSourceClimbs {
public:
    using Summary = typename Fold::Summary;

    explicit FoldedRowSourceClimbs(ClimbsFrom climbsFrom);
    FoldedRowSourceClimbs(ClimbsFrom climbsFrom, const Fold& fold);
    ~FoldedRowSourceClimbs();

    Value climb(Value column);
    Value climb(Value column, Summary& summary);
    void forget(Operation* op);
    void clear();

    const Fold& getFold() const { return _fold; }

private:
    struct Climb {
        Value _source;
        [[no_unique_address]] Summary _summary;
    };

    ClimbsFrom _climbsFrom;
    Fold _fold;
    llvm::DenseMap<Value, Climb> _climbs;
};

using RowSourceClimbs = FoldedRowSourceClimbs<NoStepSummary>;

template <typename Fold>
FoldedRowSourceClimbs<Fold>::FoldedRowSourceClimbs(ClimbsFrom climbsFrom)
    : _climbsFrom(climbsFrom)
{
}

template <typename Fold>
FoldedRowSourceClimbs<Fold>::FoldedRowSourceClimbs(ClimbsFrom climbsFrom, const Fold& fold)
    : _climbsFrom(climbsFrom),
    _fold(fold)
{
}

template <typename Fold>
FoldedRowSourceClimbs<Fold>::~FoldedRowSourceClimbs() {
}

template <typename Fold>
Value FoldedRowSourceClimbs<Fold>::climb(Value column) {
    Summary summary;
    return climb(column, summary);
}

template <typename Fold>
Value FoldedRowSourceClimbs<Fold>::climb(Value column, Summary& summary) {
    llvm::SmallVector<Value> climbing;
    Value climbed = column;
    Summary climbedSummary {};

    while (true) {
        const auto climbIt = _climbs.find(climbed);
        if (climbIt != _climbs.end()) {
            climbedSummary = climbIt->second._summary;
            climbed = climbIt->second._source;
            break;
        }

        const Value source = stepToRowSource(climbed, _climbsFrom);
        if (!source) {
            break;
        }

        climbing.push_back(climbed);
        climbed = source;
    }

    for (const Value value : llvm::reverse(climbing)) {
        climbedSummary = _fold.combine(_fold.step(value), climbedSummary);
        _climbs[value] = Climb {climbed, climbedSummary};
    }

    summary = climbedSummary;

    return climbed;
}

template <typename Fold>
void FoldedRowSourceClimbs<Fold>::forget(Operation* op) {
    visitDefinedValues(op, [this](Value value) {
        _climbs.erase(value);
    });
}

template <typename Fold>
void FoldedRowSourceClimbs<Fold>::clear() {
    _climbs.clear();
    _fold.clear();
}

}
