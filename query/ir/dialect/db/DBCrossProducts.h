#pragma once

#include <stddef.h>
#include <memory>
#include <utility>

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "IRConstantColumn.h"
#include "DBOps.h"
#include "DBRowCone.h"

namespace mlir::db {

// The columns a cross_product factor yields, in result order.
Operation::operand_range factorYieldColumns(mlir::Region& factor);

Value productFactorColumn(CrossProduct product, size_t resultIndex);

size_t leftFactorWidth(CrossProduct product);

void collectFactorTypes(Region& leftFactor, Region& rightFactor, llvm::SmallVectorImpl<Type>& types);

struct FactorReads {
    llvm::SmallVector<size_t, 4> _leftResults;
    llvm::SmallVector<size_t, 4> _rightResults;
    bool _readsElsewhere {false};
};

struct ConjunctCone {
    MaskCone _cone;
    FactorReads _reads;
};

// The cones and the factor reads the placement matchers derive from the IR, shared between
// matches until a rewrite changes the IR under them
class ConeCache {
public:
    bool yieldsConstantColumn(Value column);

    const ConjunctCone& getConjunctCone(Value conjunct, CrossProduct product);

    // Each filter column climbs once, so the columns of a stack of N filters cost N steps
    // in all rather than N per column
    Value climbFilters(Value column);

    void clear();

private:
    llvm::DenseMap<Value, bool> _constantColumns;
    llvm::DenseMap<std::pair<Value, Operation*>, std::unique_ptr<ConjunctCone>> _conjunctCones;
    llvm::DenseMap<Value, Value> _climbedColumns;
};

void collectFactorReads(const MaskCone& cone, CrossProduct product, ConeCache& cache, FactorReads& reads);

// The rows of a cross product, or of another op making rows, and of the filters stacked
// over it, climbed one filter at a time. It answers matchFilterChain's question for each
// filter of the stack, while following the readers of each level once rather than once per
// filter above it.
class FilterStack {
public:
    explicit FilterStack(Operation* rows);

    FilterOp getReader() const;

    // Whether nothing but the stack's filters and @param reader sees the rows of the stack
    bool isExclusiveTo(FilterOp reader) const;

    void climbTo(FilterOp reader);

private:
    Block* _block {nullptr};
    Operation* _top {nullptr};
    llvm::SmallPtrSet<Operation*, 16> _visited;
    llvm::SmallPtrSet<Operation*, 4> _pendingFilters;
    bool _readElsewhere {false};

    bool readsTheTop(FilterOp filter) const;

    void followReaders(Operation* level);
};

// The db.yield ending a factor region of a two-factor op - db.cross_product, db.hash_join.
Yield factorYield(Operation* op, unsigned factorIndex);

// One side of a join: the factor supplying its key, the product column the key is read
// from, the ops computing one from the other, and - once sunk - where the key sits in the
// factor's yield.
struct JoinKeySide {
    Region* _factor {nullptr};
    Value _key;
    Value _column;
    MaskCone _cone;
};

// Whether a key's rows can be hashed. The two keys need not share a type: the join hashes
// each side by its own and compares them as the equality does. A path has no value to hash,
// as two paths are equal when they run through the same nodes and edges.
bool isHashableKey(const JoinKeySide& side);

}
