#pragma once

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/DenseSet.h"

#include "DBOps.h"

namespace mlir::db {

// The labels an edge scan reads each endpoint by, null for an endpoint it reads every node
// of, and whether the index it walks is keyed by the target's label set or the source's.
struct EdgeScanLabels {
    ArrayAttr _source;
    ArrayAttr _target;
    bool _indexedByTarget {false};
};

bool readEdgeScanLabels(Operation* op, EdgeScanLabels& scanLabels);

void addSharedLabelNames(ArrayAttr alternatives, llvm::DenseSet<Attribute>& labels);

// The labels every node of a column carries, known from where its rows come from: the
// by-label read that made them and each label filter they passed on the way.
void collectKnownLabels(Value column, llvm::DenseSet<Attribute>& labels);

}
