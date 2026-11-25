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
    PatternReplay(const SeedPattern& pattern, mlir::OpBuilder& builder, Location loc)
        : _pattern(pattern),
        _builder(builder),
        _loc(loc)
    {
    }

    void bind(Value origin, Value current) {
        if (!_current.count(origin)) {
            _inFlight.push_back(origin);
        }

        _current[origin] = current;
    }

    Value currentOf(Value origin) const { return _current.lookup(origin); }

    void setJunction(const PatternJunction* junction) { _junction = junction; }

    // The value a result of the junction holds over the rebuilt rows
    Value materializeJunctionResult(Value result) {
        return materialize(junctionSource(*_junction, result));
    }

    // The value a pattern column holds over the rebuilt rows
    Value materialize(Value value) {
        mlir::IRMapping materialized;
        llvm::SmallVector<Value> worklist {value};

        while (!worklist.empty()) {
            const Value pending = worklist.back();
            if (materialized.contains(pending)) {
                worklist.pop_back();
                continue;
            }

            const Value origin = patternOrigin(pending, _pattern);
            Operation* const def = pending.getDefiningOp();

            Operation* computing = nullptr;
            if (_current.count(origin)) {
                materialized.map(pending, _current.lookup(origin));
            } else if (def && _pattern._ops.contains(def)) {
                computing = origin.getDefiningOp();

                const bool computedPerRow = computing && _pattern._ops.contains(computing) && computesPerRow(computing);
                bioassert(computedPerRow, "A pattern column read before the step binding it");
            } else if (!def || isVisibleHere(pending)) {
                materialized.map(pending, pending);
            } else {
                computing = def;
                bioassert(computesFromTheRow(def), "A walk rebuilt out of its factor reads a row the factor made");
            }

            if (computing) {
                bool operandsPending = false;
                for (const Value operand : llvm::reverse(computing->getOperands())) {
                    if (!materialized.contains(operand)) {
                        worklist.push_back(operand);
                        operandsPending = true;
                    }
                }

                if (operandsPending) {
                    continue;
                }

                Operation* const cloned = _builder.clone(*computing, materialized);
                materialized.map(pending, cloned->getResult(cast<OpResult>(origin).getResultNumber()));
            }

            worklist.pop_back();
        }

        return materialized.lookup(value);
    }

    void walk(llvm::ArrayRef<WalkEvent> events) {
        for (const WalkEvent& event : events) {
            switch (event._kind) {
                case WalkEvent::Kind::RootLabels:
                    applyRootLabels();
                break;
                case WalkEvent::Kind::Filter: {
                    FilterOp filter = event._filter;
                    keepRows(materialize(filter.getMask()));
                }
                break;
                case WalkEvent::Kind::Forward:
                    walkForward(*event._step);
                break;
                case WalkEvent::Kind::Backward:
                    walkBackward(*event._step);
                break;
                case WalkEvent::Kind::Cross:
                    crossJunction();
                break;
                case WalkEvent::Kind::Join:
                    keepRows(materializeJoin(*event._join));
                break;
            }
        }
    }

private:
    const SeedPattern& _pattern;
    mlir::OpBuilder& _builder;
    Location _loc;
    llvm::DenseMap<Value, Value> _current;
    llvm::SmallVector<Value> _inFlight;
    const PatternJunction* _junction {nullptr};

    // A walk crossing a product in leaves the factor it was written in, whose values the rows
    // rebuilt after the cross no longer see
    bool isVisibleHere(Value value) const {
        Region* const here = _builder.getInsertionBlock()->getParent();
        return value.getParentRegion()->isAncestor(here);
    }

    // The rows rebuilt so far take the junction's place: crossed with the other factor of its
    // product, or repeated once per element of its unwind
    void crossJunction() {
        const PatternJunction& junction = *_junction;
        Operation* const junctionOp = junction._op;

        llvm::SmallVector<Value> rebuilt;
        carried(rebuilt, Value());

        _builder.setInsertionPoint(junctionOp);

        Operation* crossed = nullptr;
        llvm::SmallVector<Value> otherColumns;
        llvm::SmallVector<Value> rebuiltColumns;

        if (CrossProduct product = dyn_cast<CrossProduct>(junctionOp)) {
            Region& leftFactor = product.getLeftFactor();
            Region& rightFactor = product.getRightFactor();
            const bool patternOnTheLeft = junction._otherFactor == &rightFactor;

            Region& patternFactor = patternOnTheLeft ? leftFactor : rightFactor;
            Yield yield = cast<Yield>(patternFactor.front().getTerminator());
            yield->setOperands(rebuilt);

            llvm::SmallVector<Type> types;
            collectFactorTypes(leftFactor, rightFactor, types);

            CrossProduct crossedProduct = _builder.create<CrossProduct>(_loc, types);
            crossedProduct.getLeftFactor().takeBody(leftFactor);
            crossedProduct.getRightFactor().takeBody(rightFactor);

            const size_t leftWidth = leftFactorWidth(crossedProduct);
            for (const OpResult result : crossedProduct->getResults()) {
                const bool fromThePattern = (result.getResultNumber() < leftWidth) == patternOnTheLeft;
                (fromThePattern ? rebuiltColumns : otherColumns).push_back(result);
            }

            crossed = crossedProduct.getOperation();
        } else {
            Unwind unwind = cast<Unwind>(junctionOp);

            llvm::SmallVector<Type> types {unwind.getElement().getType()};
            for (const Value column : rebuilt) {
                types.push_back(column.getType());
            }

            Unwind crossedUnwind = _builder.create<Unwind>(_loc, types, unwind.getSource(), rebuilt);
            const ResultRange crossedRows = crossedUnwind.getCarried();

            otherColumns.push_back(crossedUnwind.getElement());
            rebuiltColumns.assign(crossedRows.begin(), crossedRows.end());

            crossed = crossedUnwind.getOperation();
        }

        _builder.setInsertionPointAfter(crossed);

        const llvm::SmallVector<Value> rebuiltOrigins(_inFlight.begin(), _inFlight.end());
        _inFlight.clear();
        _current.clear();

        for (size_t index = 0; index < otherColumns.size(); index++) {
            bind(junction._otherResults[index], otherColumns[index]);
        }

        for (size_t index = 0; index < rebuiltColumns.size(); index++) {
            bind(rebuiltOrigins[index], rebuiltColumns[index]);
        }
    }

    Value materializeJoin(const JoinConjunct& join) {
        mlir::IRMapping mapping;
        for (const Value input : join._cone._inputs) {
            const Value source = junctionSource(*_junction, climbFilters(input));
            if (source) {
                mapping.map(input, materialize(source));
            }
        }

        for (Operation* const coneOp : join._cone._ops) {
            _builder.clone(*coneOp, mapping);
        }

        return mapping.lookup(join._conjunct);
    }

    // A hop hands back the node it walks from as its own result, so it does not carry it
    void carried(llvm::SmallVectorImpl<Value>& columns, Value walkedFrom) const {
        for (const Value origin : _inFlight) {
            if (origin != walkedFrom) {
                columns.push_back(_current.lookup(origin));
            }
        }
    }

    void carried(llvm::SmallVectorImpl<Value>& columns, llvm::SmallVectorImpl<Type>& types, Value walkedFrom) const {
        const size_t first = columns.size();
        carried(columns, walkedFrom);
        for (size_t index = first; index < columns.size(); index++) {
            types.push_back(columns[index].getType());
        }
    }

    void rebindCarried(Operation* op, size_t firstCarried, Value walkedFrom) {
        size_t resultIndex = firstCarried;
        for (const Value origin : _inFlight) {
            if (origin != walkedFrom) {
                _current[origin] = op->getResult(resultIndex++);
            }
        }
    }

    void keepRows(Value mask) {
        llvm::SmallVector<Value> columns;
        llvm::SmallVector<Type> types;
        carried(columns, types, Value());

        FilterOp filter = _builder.create<FilterOp>(_loc, types, mask, columns);
        rebindCarried(filter.getOperation(), 0, Value());
    }

    void applyRootLabels() {
        ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(_pattern._root);
        if (!scanByLabel) {
            return;
        }

        const Value root = _current.lookup(_pattern._root->getResult(0));
        keepRows(checkLabels(root, scanByLabel.getLabels(), _loc, _builder));
    }

    void bindHop(const PatternStep& step, Operation* walked, Value walkedFrom, size_t fromResult, size_t toResult) {
        rebindCarried(walked, hopFixedResultCount, walkedFrom);

        bind(step._from, walked->getResult(fromResult));
        bind(step._to, walked->getResult(toResult));
        bind(step._op->getResult(1), walked->getResult(1));
        bind(step._op->getResult(2), walked->getResult(2));
    }

    void walkForward(const PatternStep& step) {
        if (ExplorePaths exploration = dyn_cast<ExplorePaths>(step._op)) {
            walkExploration(exploration, step);
            return;
        }

        llvm::SmallVector<Value> columns;
        llvm::SmallVector<Type> types;
        hopTypes(step._op, types);
        carried(columns, types, step._from);

        OperationState state(_loc, step._op->getName());
        state.addOperands(_current.lookup(step._from));
        state.addOperands(columns);
        state.addTypes(types);
        Operation* const walked = _builder.create(state);

        bindHop(step, walked, step._from, hopInputResult(step._op), hopReachedResult(step._op));
    }

    // An out-hop reversed is an in-hop from the node it reached, over the same edges
    void walkBackward(const PatternStep& step) {
        llvm::SmallVector<Value> columns;
        carried(columns, step._to);

        const Value from = _current.lookup(step._to);
        Operation* const walked = createReversedHop(step._op, from, ArrayAttr(), columns, _builder);

        bindHop(step, walked, step._to, hopReachedResult(walked), hopInputResult(walked));
    }

    void walkExploration(ExplorePaths exploration, const PatternStep& step) {
        llvm::SmallVector<Value> columns;
        llvm::SmallVector<Type> types {exploration.getSrcids().getType(), exploration.getTgtids().getType(), exploration.getPaths().getType()};
        carried(columns, types, step._from);

        ExplorePaths walked = _builder.create<ExplorePaths>(_loc,
                                                            types,
                                                            _current.lookup(step._from),
                                                            columns,
                                                            Value(),
                                                            ValueRange(),
                                                            exploration.getDirectionsAttr(),
                                                            exploration.getMinHopsAttr(),
                                                            exploration.getMaxHopsAttr(),
                                                            exploration.getEdgeTypesAttr(),
                                                            exploration.getEndLabelsAttr(),
                                                            exploration.getHopLabelsAttr(),
                                                            IntegerAttr(),
                                                            UnitAttr(),
                                                            exploration.getDistinctAttr(),
                                                            exploration.getStepCount());
        takeHopRegions(exploration, walked);

        rebindCarried(walked.getOperation(), pathFixedResultCount, step._from);
        bind(step._from, walked.getSrcids());
        bind(step._to, walked.getTgtids());
        bind(exploration.getPaths(), walked.getPaths());
    }
};

// The junction a pattern ends at, the conjuncts of the filters over it that relate the
// pattern to the junction's other side, and the rows that side multiplies the walk's by
struct JunctionJoins {
    PatternJunction _junction;
    llvm::SmallVector<JoinConjunct, 2> _joins;
    double _crossedRows {0};
};

}
