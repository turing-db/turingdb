#pragma once

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/STLFunctionalExtras.h"
#include "llvm/ADT/SetVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

namespace mlir::db {

struct MaskCone {
    llvm::SmallVector<Operation*> _ops;
    llvm::SmallSetVector<Value, 4> _inputs;
};

// A cone is what a pass moves onto rows the query may not have run it on, so an op that may
// raise on some row's value - any op not declared speculatable, or holding one - stays out
// of it, as one of the cone's inputs
bool raisesOnSomeRows(Operation* op);

// A row-wise op: one output row per input row, computed from that row alone, so a relation
// deduplicated before it yields the same set after it.
bool isRowWiseOp(Operation* op);

// A row-wise op a re-rooted pattern recomputes over its new rows rather than carrying
bool computesPerRow(Operation* op);

bool computesFromTheRow(Operation* op);

// Gathers the mask's compute cone, the ops @param joinsTheCone accepts, and the external
// columns feeding it. Valid even when the cone spans blocks.
void collectConePostOrder(Value mask,
                          llvm::function_ref<bool(Operation*)> joinsTheCone,
                          llvm::SmallPtrSet<Operation*, 8>& visited,
                          MaskCone& cone);

void collectRowCone(llvm::ArrayRef<Value> roots, Block* block, MaskCone& cone);

}
