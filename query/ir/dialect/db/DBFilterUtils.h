#pragma once

#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"

namespace mlir::db {

void bypassFilter(FilterOp filter);

bool isUntypedNullMask(Value mask);

// An `and` holds a bool on every row, as codegen types it, but over two null literals, where it
// is the null itself: a second one adds nothing to the mask
Value conjoin(llvm::ArrayRef<Value> conjuncts, Location loc, mlir::RewriterBase& rewriter);

void eraseUnusedConjunction(Value mask, mlir::RewriterBase& rewriter);

// Drops a conjunct the filter's rows now hold without it from its mask, and the ops that
// computed nothing else. The `and` holding it then holds what its other side does on every
// row, so it gives way to that side. A conjunct read more than once, or a null literal on
// that side, which could meet another in an `and`, leaves the others to be joined again.
void dropConjunct(FilterOp filter, Value conjunct, mlir::RewriterBase& rewriter);

void dropConjuncts(FilterOp filter, llvm::ArrayRef<Value> conjuncts, mlir::RewriterBase& rewriter);

// The conjuncts of a mask, as an `and` tree spells them: the predicates that all have to
// hold, one of which can become the fused scan while the others stay a filter.
void collectConjuncts(Value mask, llvm::SmallVectorImpl<Value>& conjuncts);

// Whether @param matchConjunct accepts a conjunct of @param filter's mask, tried in their order
template <typename MatchConjunct>
bool matchAConjunct(FilterOp filter, MatchConjunct matchConjunct) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    return llvm::any_of(conjuncts, matchConjunct);
}

// Whether nothing but @param filter reads the rows @param op makes, but the row computations
// of its mask, so a rewrite cutting those rows changes what no other op sees. A computation
// nothing reads is a reader of its own: it may raise on a row the cut takes away.
bool readsOnlyIntoTheFilter(Operation* op, FilterOp filter);

using OpAnswers = llvm::SmallDenseMap<Operation*, bool, 2>;

// The answer @param compute gives about @param op, worked out once however many conjuncts of
// one mask ask about the same op
template <typename Compute>
bool answerOnce(OpAnswers& answers, Operation* op, Compute compute) {
    const auto [found, inserted] = answers.try_emplace(op, false);
    if (inserted) {
        found->second = compute();
    }

    return found->second;
}

// The op making every column @param filter cuts, null when they come from more than one
Operation* filteredSource(FilterOp filter);

bool keepsTheSourceRows(Operation& op);

// Whether the rows the source starts reach the filter only through ops keepsTheRows accepts,
// with nothing else reading them before it
bool rowsReachTheFilter(Operation* source, FilterOp filter, bool (*keepsTheRows)(Operation&));

bool isEqualityOfTheFilter(EqOp equality, FilterOp filter);

}
