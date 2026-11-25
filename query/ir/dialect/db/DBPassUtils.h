#pragma once

#include <stddef.h>
#include <stdint.h>

#include "mlir/IR/Builders.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"
#include "DBPasses.h"
#include "DBRowCone.h"

namespace mlir::db {

extern const DBPassContext defaultPassContext;

void eraseIfUnused(Operation* op);

bool isNodeSource(Operation* op);

bool isEdgeHop(Operation* op);

bool isEdgeScan(Operation* op);

bool isReverseHop(Operation* op);

// The result of a hop holding the node it walks from, and the one holding the node it reaches
size_t hopInputResult(Operation* hop);

size_t hopReachedResult(Operation* hop);

constexpr size_t hopFixedResultCount = 4;

constexpr size_t EDGE_IDS_RESULT = 1;
constexpr size_t EDGE_TYPES_RESULT = 2;
constexpr size_t EDGE_TARGETS_RESULT = 3;

// srcids, tgtids and paths; the carry set follows
constexpr size_t pathFixedResultCount = 3;

bool isNodeColumn(Value column);

bool isEdgeColumn(Value column);

// The node or edge type a column holds: its own element, or the element of the list of nodes
// or edges a list index, head() or last() reads one entry of. Null for any other column.
Type entityElementOf(Value column);

bool isEntityColumn(Value column);

MaskCone collectMaskCone(Value mask);

Value checkLabels(Value nodes, ArrayAttr labels, Location loc, mlir::OpBuilder& builder);

// Cuts the columns to the rows whose first column holds a node carrying every label
void keepLabelledRows(llvm::SmallVectorImpl<Value>& columns, ArrayAttr labels, Location loc, mlir::OpBuilder& builder);

// The ops computing one value per row out of the row's columns, through a region of their
// own included, which compute the same values over any run of the rows.
bool computesAValuePerRow(Operation* op);

void cloneRowCone(llvm::ArrayRef<Operation*> ops, mlir::IRMapping& mapping, mlir::RewriterBase& rewriter);

void eraseUnusedRowCone(llvm::ArrayRef<Operation*> ops, mlir::RewriterBase& rewriter);

// Moves the ops after @param anchor in its block that compute @param values to just before
// @param anchor. The values are constants, which a WITH after @param anchor may bind, and the
// ops computing a constant read only constants.
void hoistDefinitionsAhead(llvm::ArrayRef<Value> values, Operation* anchor, mlir::RewriterBase& rewriter);

// Erases @param root if nothing reads it, then each row computation it read that nothing
// else reads. A handle the caller holds to an op of that cone may be freed afterwards.
void eraseUnusedRowConeOf(Operation* root, mlir::RewriterBase& rewriter);

void eraseIfUnused(Operation* op, mlir::RewriterBase& rewriter);

void hopTypes(Operation* hop, llvm::SmallVectorImpl<Type>& types);

// The hop over @param hop's edges walked the other way, from @param from, asking the node it
// reaches for @param labels. The labels @param hop asked of its own reached node are dropped,
// since that node is now the one walked from; get_edges walks both ways and asks for none.
Operation* createReversedHop(Operation* hop, Value from, ArrayAttr labels, ValueRange carried, mlir::OpBuilder& builder);

Type boolColumnType(mlir::MLIRContext* context);

IntegerAttr unsignedAttribute(uint64_t value, mlir::OpBuilder& builder);

void takeHopRegions(ExplorePaths from, ExplorePaths to);

Value climbFilters(Value column);

// The operand of the exploration a column of its rows repeats for every path of a seed, or
// null for a column the walk binds
Value seedColumnOf(ExplorePaths exploration, Value column);

bool isHop(Operation* op);

}
