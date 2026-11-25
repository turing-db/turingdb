#pragma once

#include <stdint.h>

#include "mlir/IR/Builders.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"

namespace mlir::db {

// A bound value is run-invariant: ops that cannot raise compute it from parameters and
// literals alone, so it holds the same value wherever it is computed
void computeBoundValuesAhead(Operation* lookup);

// The node or edge column a side of an id comparison names, compared as itself or through id()
Value comparedEntityColumn(Value side);

// The ID of the node or edge a literal names, as id(e) = literal compares them
bool entityIDOfLiteral(Attribute literal, int64_t& entityID);

// The IDs a disjunction over a node or edge column names: the ones the query spells, and the
// columns holding the ones bound when it runs, each an ID or a list of them
struct EntityIDList {
    llvm::SmallVector<int64_t> _literals;
    llvm::SmallVector<Value, 2> _bound;
};

// An UNWIND of N elements folds into a chain of N - 1 ORs, so the walk keeps its own stack:
// recursing once per OR overflows the thread's stack on a list of a few thousand.
bool collectIDDisjunction(Value mask, Value scanColumn, EntityIDList& entityIDs);

// eq(get_node_properties(column, property), value), with the value on either side: a
// constant, or a column bound before the query runs
struct PropertyEquality {
    GetNodeProperties _read;
    TypedAttr _value;
    Value _boundValue;
};

bool matchPropertyEquality(EqOp equality, PropertyEquality& match);

Value disjunctionColumn(Value mask);

Value nodeDisjunctionColumn(Value mask);

// A column holding the first or the last edge of each path of a walk: the walk's edge list
// read at index 0 or -1, or by head() or last(). A list written backwards holds the walk's
// last hop first.
struct WalkEndEdge {
    ExplorePaths _walk;
    bool _first {false};
};

bool matchWalkEndEdge(Value column, WalkEndEdge& endEdge);

ConstScanEdges scanListedEdges(const EntityIDList& edgeIDs, bool undirected, Location loc, mlir::OpBuilder& builder);

// The end a hop of @param direction walks each listed edge from, or with @param reached the
// end it reaches: a hop walking both ways walks every edge from each of its ends
Value scanListedEdgeEnds(const EntityIDList& edgeIDs, storage::PathDirection direction, bool reached, Location loc, mlir::OpBuilder& builder);

}
