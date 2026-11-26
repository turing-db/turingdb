#pragma once

#include <stddef.h>

#include "mlir/IR/Builders.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"
#include "DBCrossProducts.h"
#include "DBPassUtils.h"
#include "DBRowCone.h"

#include "BioAssert.h"

namespace mlir::db {

// A pattern hop or walk: the origin of the node it starts from and of the node it reaches
struct PatternStep {
    Operation* _op {nullptr};
    Value _from;
    Value _to;
};

// The ops a pattern is built of, from the scan at its root: hops, walks, the filters
// constraining what they bind, and the row-wise ops those filters read
struct SeedPattern {
    Operation* _root {nullptr};
    llvm::SmallPtrSet<Operation*, 32> _ops;
    llvm::SmallVector<PatternStep> _steps;
    llvm::SmallVector<FilterOp> _filters;
};

// Where a pattern's rows meet rows that are not its own: a cross product with the pattern
// in one factor, or an unwind carrying the pattern's columns
struct PatternJunction {
    Operation* _op {nullptr};
    Region* _otherFactor {nullptr};
    llvm::SmallVector<Value> _patternOutputs;
    llvm::SmallVector<Value> _patternResults;
    llvm::SmallVector<Value> _otherResults;
};

void readProductJunction(CrossProduct product, bool patternOnTheLeft, PatternJunction& junction);

void readUnwindJunction(Unwind unwind, PatternJunction& junction);

// The value a result of the junction hands on: the pattern column it took, or for the other
// side the result itself. Null for a value the junction did not make.
Value junctionSource(const PatternJunction& junction, Value result);

// The unwound list is none of the pattern's, the unwind carries the pattern's columns alone, and
// nothing but the pattern and the unwind reads what the pattern binds
bool unwindsOverThePatternAlone(const PatternJunction& junction,
                                const SeedPattern& pattern,
                                llvm::ArrayRef<Operation*> orderedOps);

bool standsFromTheJunction(Operation* op, const PatternJunction& junction);

// A conjunct of a filter over a junction that reads both its sides, which the pattern's
// walk can apply as soon as it has bound the pattern nodes and edges the conjunct reads.
// _equatesKeys says it is an equality of a key of each side, and _hashes that
// fuse_hash_join can then run a product crossed in right before it as a hash join.
struct JoinConjunct {
    Value _conjunct;
    FilterOp _filter {nullptr};
    MaskCone _cone;
    llvm::SmallPtrSet<void*, 4> _patternInputs;
    bool _equatesKeys {false};
    bool _hashes {false};
};

struct WalkEvent {
    enum class Kind {
        RootLabels,
        Filter,
        Forward,
        Backward,
        Cross,
        Join,
    };

    Kind _kind {Kind::Filter};
    FilterOp _filter {nullptr};
    const PatternStep* _step {nullptr};
    const JoinConjunct* _join {nullptr};
};

// The value a pattern column was bound as, following the ops that only hand it on
Value patternOrigin(Value column, const SeedPattern& pattern);

void collectPatternInputs(Value value, const SeedPattern& pattern, llvm::SmallPtrSetImpl<void*>& inputs);

Operation* findPatternRoot(Value column);

void collectPatternOps(Operation* root, Operation* end, SeedPattern& pattern);

bool walksFreely(ExplorePaths exploration);

// Checks every op of the pattern is one a re-rooted pattern can be rebuilt from, and
// records its steps and filters
bool analyzePattern(SeedPattern& pattern, llvm::ArrayRef<Operation*> orderedOps);

// Whether the pattern can be walked out from the seeded node, or from both ends of the hop
// @param fetchedStep: every hop reached from one of its ends, a walk only ever from the node
// it starts at
bool isWalkableFrom(const SeedPattern& pattern, Value seeded, const PatternStep* fetchedStep);

const PatternStep* findEdgeStep(const SeedPattern& pattern, Value origin);

bool isPatternNode(const SeedPattern& pattern, Value origin);

void bindStep(const PatternStep& step, llvm::DenseSet<Value>& bound);

// The order a pattern is rebuilt in from its seeded node, or from the hop @param fetchedStep
// binds whole: each filter as soon as what it reads is bound, then the first step written
// that leaves a bound node. The rows already pass the filters of @param applied, and the
// root's labels when @param rootChecked. False when the walk leaves a step or a filter out.
bool planWalk(const SeedPattern& pattern,
              Value seeded,
              const PatternStep* fetchedStep,
              const llvm::SmallPtrSetImpl<Operation*>& applied,
              bool rootChecked,
              llvm::SmallVectorImpl<WalkEvent>& events);

// Rebuilds a pattern's rows from its seeded node, carrying every column bound so far
class PatternReplay {
public:
    PatternReplay(const SeedPattern& pattern, mlir::OpBuilder& builder, Location loc);

    void bind(Value origin, Value current);

    Value currentOf(Value origin) const { return _current.lookup(origin); }

    void setJunction(const PatternJunction* junction) { _junction = junction; }

    // The value a result of the junction holds over the rebuilt rows
    Value materializeJunctionResult(Value result);

    // The value a pattern column holds over the rebuilt rows
    Value materialize(Value value);

    void walk(llvm::ArrayRef<WalkEvent> events);

private:
    const SeedPattern& _pattern;
    mlir::OpBuilder& _builder;
    Location _loc;
    llvm::DenseMap<Value, Value> _current;
    llvm::SmallVector<Value> _inFlight;
    const PatternJunction* _junction {nullptr};

    // A walk crossing a product in leaves the factor it was written in, whose values the rows
    // rebuilt after the cross no longer see
    bool isVisibleHere(Value value) const;

    // The rows rebuilt so far take the junction's place: crossed with the other factor of its
    // product, or repeated once per element of its unwind
    void crossJunction();

    Value materializeJoin(const JoinConjunct& join);

    // A hop hands back the node it walks from as its own result, so it does not carry it
    void carried(llvm::SmallVectorImpl<Value>& columns, Value walkedFrom) const;

    void carried(llvm::SmallVectorImpl<Value>& columns, llvm::SmallVectorImpl<Type>& types, Value walkedFrom) const;

    void rebindCarried(Operation* op, size_t firstCarried, Value walkedFrom);

    void keepRows(Value mask);

    void applyRootLabels();

    void bindHop(const PatternStep& step, Operation* walked, Value walkedFrom, size_t fromResult, size_t toResult);

    void walkForward(const PatternStep& step);

    // An out-hop reversed is an in-hop from the node it reached, over the same edges
    void walkBackward(const PatternStep& step);

    void walkExploration(ExplorePaths exploration, const PatternStep& step);
};

// The junction a pattern ends at, the conjuncts of the filters over it that relate the
// pattern to the junction's other side, and the rows that side multiplies the walk's by
struct JunctionJoins {
    PatternJunction _junction;
    llvm::SmallVector<JoinConjunct, 2> _joins;
    double _crossedRows {0};
};

}
