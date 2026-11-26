#include "DBPatternWalk.h"

using namespace mlir;
using namespace mlir::db;

namespace {

size_t eventRank(const WalkEvent& event) {
    if (event._kind == WalkEvent::Kind::RootLabels) {
        return 1;
    }

    FilterOp filter = event._filter;
    const Value mask = filter.getMask();

    if (mask.getDefiningOp<CheckEdgeTypeConstraint>()) {
        return 0;
    } else if (mask.getDefiningOp<CheckLabelConstraint>()) {
        return 1;
    }

    return 2;
}

bool raisesOnSomeRow(Value mask, const SeedPattern& pattern) {
    llvm::SmallVector<Value> pending {mask};
    llvm::DenseSet<Value> visited;

    while (!pending.empty()) {
        const Value current = pending.pop_back_val();
        if (!visited.insert(current).second) {
            continue;
        }

        Operation* const def = current.getDefiningOp();
        if (!def || !pattern._ops.contains(def) || !computesPerRow(def)) {
            continue;
        } else if (raisesOnSomeRows(def)) {
            return true;
        }

        llvm::append_range(pending, def->getOperands());
    }

    return false;
}

}

void mlir::db::readProductJunction(CrossProduct product, bool patternOnTheLeft, PatternJunction& junction) {
    Operation* const op = product.getOperation();
    const size_t leftCount = factorYield(op, 0).getNumOperands();

    Region& patternFactor = patternOnTheLeft ? product.getLeftFactor() : product.getRightFactor();

    junction._op = op;
    junction._otherFactor = patternOnTheLeft ? &product.getRightFactor() : &product.getLeftFactor();

    const Operation::operand_range patternYield = factorYieldColumns(patternFactor);
    junction._patternOutputs.assign(patternYield.begin(), patternYield.end());

    for (size_t index = 0; index < op->getNumResults(); index++) {
        const bool fromThePattern = (index < leftCount) == patternOnTheLeft;
        (fromThePattern ? junction._patternResults : junction._otherResults).push_back(op->getResult(index));
    }
}

void mlir::db::readUnwindJunction(Unwind unwind, PatternJunction& junction) {
    const Operation::operand_range patternOutputs = unwind.getColumnsToFilter();
    const ResultRange patternResults = unwind.getCarried();

    junction._op = unwind.getOperation();
    junction._otherResults.push_back(unwind.getElement());
    junction._patternOutputs.assign(patternOutputs.begin(), patternOutputs.end());
    junction._patternResults.assign(patternResults.begin(), patternResults.end());
}

Value mlir::db::junctionSource(const PatternJunction& junction, Value result) {
    const auto patternResult = llvm::find(junction._patternResults, result);
    if (patternResult != junction._patternResults.end()) {
        return junction._patternOutputs[patternResult - junction._patternResults.begin()];
    } else if (llvm::is_contained(junction._otherResults, result)) {
        return result;
    }

    return {};
}

bool mlir::db::unwindsOverThePatternAlone(const PatternJunction& junction,
                                          const SeedPattern& pattern,
                                          llvm::ArrayRef<Operation*> orderedOps) {
    Unwind unwind = cast<Unwind>(junction._op);

    Operation* const sourceDef = unwind.getSource().getDefiningOp();
    if (sourceDef && pattern._ops.contains(sourceDef)) {
        return false;
    }

    for (const Value output : junction._patternOutputs) {
        Operation* const def = output.getDefiningOp();
        if (!def || !pattern._ops.contains(def)) {
            return false;
        }
    }

    for (Operation* const patternOp : orderedOps) {
        for (Operation* const user : patternOp->getUsers()) {
            if (user != junction._op && !pattern._ops.contains(user)) {
                return false;
            }
        }
    }

    return true;
}

bool mlir::db::standsFromTheJunction(Operation* op, const PatternJunction& junction) {
    if (!op || op->getBlock() != junction._op->getBlock()) {
        return false;
    }

    return !op->isBeforeInBlock(junction._op);
}

Value mlir::db::patternOrigin(Value column, const SeedPattern& pattern) {
    for (;;) {
        const OpResult result = dyn_cast<OpResult>(column);
        if (!result || !pattern._ops.contains(result.getOwner())) {
            return column;
        }

        Operation* const op = result.getOwner();
        const size_t resultIndex = result.getResultNumber();

        if (FilterOp filter = dyn_cast<FilterOp>(op)) {
            column = filter.getColumnsToFilter()[resultIndex];
        } else if (isHop(op) && resultIndex >= hopFixedResultCount) {
            column = op->getOperand(1 + resultIndex - hopFixedResultCount);
        } else if (isHop(op) && resultIndex == hopInputResult(op)) {
            column = op->getOperand(0);
        } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op); exploration && resultIndex >= pathFixedResultCount) {
            column = exploration.getColumnsToFilter()[resultIndex - pathFixedResultCount];
        } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op); exploration && resultIndex == 0) {
            column = exploration.getInputNodes();
        } else {
            return column;
        }
    }
}

void mlir::db::collectPatternInputs(Value value, const SeedPattern& pattern, llvm::SmallPtrSetImpl<void*>& inputs) {
    llvm::SmallVector<Value> worklist {value};
    llvm::DenseSet<Value> visited;

    while (!worklist.empty()) {
        const Value current = worklist.pop_back_val();
        if (!visited.insert(current).second) {
            continue;
        }

        Operation* const def = current.getDefiningOp();
        if (def && pattern._ops.contains(def) && computesPerRow(def)) {
            llvm::append_range(worklist, def->getOperands());
            continue;
        }

        const Value origin = patternOrigin(current, pattern);
        Operation* const originDef = origin.getDefiningOp();
        if (!originDef || !pattern._ops.contains(originDef)) {
            continue;
        } else if (computesPerRow(originDef)) {
            worklist.push_back(origin);
        } else {
            inputs.insert(origin.getAsOpaquePointer());
        }
    }
}

Operation* mlir::db::findPatternRoot(Value column) {
    for (;;) {
        const OpResult result = dyn_cast<OpResult>(column);
        if (!result) {
            return nullptr;
        }

        Operation* const op = result.getOwner();
        const size_t resultIndex = result.getResultNumber();

        if (isa<ScanNodes, ScanNodesByLabel>(op)) {
            return op;
        } else if (FilterOp filter = dyn_cast<FilterOp>(op)) {
            column = filter.getColumnsToFilter()[resultIndex];
        } else if (isHop(op)) {
            column = resultIndex >= hopFixedResultCount ? op->getOperand(1 + resultIndex - hopFixedResultCount) : op->getOperand(0);
        } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op)) {
            column = resultIndex >= pathFixedResultCount ? exploration.getColumnsToFilter()[resultIndex - pathFixedResultCount] : exploration.getInputNodes();
        } else {
            return nullptr;
        }
    }
}

void mlir::db::collectPatternOps(Operation* root, Operation* end, SeedPattern& pattern) {
    pattern._root = root;

    for (Operation& op : llvm::make_range(Block::iterator(root), Block::iterator(end))) {
        const bool readsThePattern = llvm::any_of(op.getOperands(), [&pattern](Value operand) {
            Operation* const def = operand.getDefiningOp();
            return def && pattern._ops.contains(def);
        });

        if (&op == root || readsThePattern) {
            pattern._ops.insert(&op);
        }
    }
}

bool mlir::db::walksFreely(ExplorePaths exploration) {
    return !exploration.getEndNodes()
        && !exploration.getEndColumn()
        && !exploration.getEndsOnSeed()
        && !exploration.getDistinct()
        && exploration.getHopImports().empty();
}

bool mlir::db::analyzePattern(SeedPattern& pattern, llvm::ArrayRef<Operation*> orderedOps) {
    if (!isa<ScanNodes, ScanNodesByLabel>(pattern._root)) {
        return false;
    }

    for (Operation* const op : orderedOps) {
        if (op == pattern._root || computesPerRow(op)) {
            continue;
        } else if (FilterOp filter = dyn_cast<FilterOp>(op)) {
            pattern._filters.push_back(filter);
        } else if (isHop(op)) {
            const Value from = patternOrigin(op->getOperand(0), pattern);
            pattern._steps.push_back(PatternStep {._op = op, ._from = from, ._to = op->getResult(hopReachedResult(op))});
        } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op)) {
            if (!walksFreely(exploration)) {
                return false;
            }

            const Value from = patternOrigin(exploration.getInputNodes(), pattern);
            pattern._steps.push_back(PatternStep {._op = op, ._from = from, ._to = exploration.getTgtids()});
        } else {
            return false;
        }
    }

    return true;
}

bool mlir::db::isWalkableFrom(const SeedPattern& pattern, Value seeded, const PatternStep* fetchedStep) {
    llvm::SmallPtrSet<void*, 16> reached {seeded.getAsOpaquePointer()};
    llvm::SmallPtrSet<Operation*, 16> walked;

    if (fetchedStep) {
        reached = {fetchedStep->_from.getAsOpaquePointer(), fetchedStep->_to.getAsOpaquePointer()};
        walked.insert(fetchedStep->_op);
    }

    bool progressed = true;
    while (progressed) {
        progressed = false;

        for (const PatternStep& step : pattern._steps) {
            if (walked.contains(step._op)) {
                continue;
            }

            const bool fromReached = reached.contains(step._from.getAsOpaquePointer());
            const bool toReached = reached.contains(step._to.getAsOpaquePointer());
            const bool reversible = isHop(step._op);

            if (fromReached || (toReached && reversible)) {
                reached.insert(step._from.getAsOpaquePointer());
                reached.insert(step._to.getAsOpaquePointer());
                walked.insert(step._op);
                progressed = true;
            }
        }
    }

    return walked.size() == pattern._steps.size();
}

const PatternStep* mlir::db::findEdgeStep(const SeedPattern& pattern, Value origin) {
    for (const PatternStep& step : pattern._steps) {
        if (isHop(step._op) && step._op->getResult(EDGE_IDS_RESULT) == origin) {
            return &step;
        }
    }

    return nullptr;
}

bool mlir::db::isPatternNode(const SeedPattern& pattern, Value origin) {
    if (origin == pattern._root->getResult(0)) {
        return true;
    }

    return llvm::any_of(pattern._steps, [origin](const PatternStep& step) { return step._to == origin; });
}

void mlir::db::bindStep(const PatternStep& step, llvm::DenseSet<Value>& bound) {
    Operation* const stepOp = step._op;

    bound.insert(step._from);
    bound.insert(step._to);

    if (ExplorePaths exploration = dyn_cast<ExplorePaths>(stepOp)) {
        bound.insert(exploration.getPaths());
    } else {
        bound.insert(stepOp->getResult(1));
        bound.insert(stepOp->getResult(2));
    }
}

bool mlir::db::planWalk(const SeedPattern& pattern,
                        Value seeded,
                        const PatternStep* fetchedStep,
                        const llvm::SmallPtrSetImpl<Operation*>& applied,
                        bool rootChecked,
                        llvm::SmallVectorImpl<WalkEvent>& events) {
    const Value root = pattern._root->getResult(0);
    llvm::DenseSet<Value> bound {seeded};
    llvm::SmallPtrSet<Operation*, 16> appliedFilters(applied.begin(), applied.end());
    llvm::SmallPtrSet<Operation*, 16> walked;

    if (fetchedStep) {
        bindStep(*fetchedStep, bound);
        walked.insert(fetchedStep->_op);
    }

    const auto readsBound = [&pattern, &bound](Value value) {
        llvm::SmallPtrSet<void*, 8> inputs;
        collectPatternInputs(value, pattern, inputs);

        return llvm::all_of(inputs, [&bound](void* input) { return bound.contains(Value::getFromOpaquePointer(input)); });
    };

    // A filter whose mask may raise on a row's value runs on no row the written pattern did
    // not run it on: only once every filter and step written ahead of it holds
    const auto holdsWhatPrecedes = [&pattern, &appliedFilters, &walked, &rootChecked](FilterOp filter) {
        Operation* const filterOp = filter.getOperation();
        const auto precedes = [filterOp](Operation* op) {
            return op->isBeforeInBlock(filterOp);
        };

        for (FilterOp written : pattern._filters) {
            if (precedes(written) && !appliedFilters.contains(written.getOperation())) {
                return false;
            }
        }

        for (const PatternStep& step : pattern._steps) {
            if (precedes(step._op) && !walked.contains(step._op)) {
                return false;
            }
        }

        return rootChecked;
    };

    bool progressed = true;
    while (progressed) {
        progressed = false;

        llvm::SmallVector<WalkEvent> checks;
        if (!rootChecked && bound.contains(root)) {
            checks.push_back(WalkEvent {._kind = WalkEvent::Kind::RootLabels});
            rootChecked = true;
        }

        for (FilterOp filter : pattern._filters) {
            const Value mask = filter.getMask();
            const bool unapplied = !appliedFilters.contains(filter.getOperation());
            const bool applicable = unapplied
                                 && readsBound(mask)
                                 && (!raisesOnSomeRow(mask, pattern) || holdsWhatPrecedes(filter));
            if (applicable) {
                checks.push_back(WalkEvent {._kind = WalkEvent::Kind::Filter, ._filter = filter});
                appliedFilters.insert(filter.getOperation());
            }
        }

        // The order codegen writes them in under a hop, which the passes fusing them into
        // the hop match on: its edge types, then its end's labels, then the rest
        llvm::stable_sort(checks, [](const WalkEvent& lhs, const WalkEvent& rhs) {
            return eventRank(lhs) < eventRank(rhs);
        });

        llvm::append_range(events, checks);

        for (const PatternStep& step : pattern._steps) {
            Operation* const stepOp = step._op;
            if (walked.contains(stepOp)) {
                continue;
            }

            const bool forward = bound.contains(step._from);
            const bool backward = !forward && bound.contains(step._to) && isHop(stepOp);
            if (!forward && !backward) {
                continue;
            }

            events.push_back(WalkEvent {._kind = forward ? WalkEvent::Kind::Forward : WalkEvent::Kind::Backward, ._step = &step});

            bindStep(step, bound);
            walked.insert(stepOp);
            progressed = true;
            break;
        }
    }

    const bool appliesEveryFilter = llvm::all_of(pattern._filters, [&appliedFilters](FilterOp filter) {
        return appliedFilters.contains(filter.getOperation());
    });

    return walked.size() == pattern._steps.size() && appliesEveryFilter;
}

PatternReplay::PatternReplay(const SeedPattern& pattern, mlir::OpBuilder& builder, Location loc)
    : _pattern(pattern),
    _builder(builder),
    _loc(loc)
{
}

void PatternReplay::bind(Value origin, Value current) {
    if (!_current.count(origin)) {
        _inFlight.push_back(origin);
    }

    _current[origin] = current;
}

Value PatternReplay::materializeJunctionResult(Value result) {
    return materialize(junctionSource(*_junction, result));
}

Value PatternReplay::materialize(Value value) {
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

void PatternReplay::walk(llvm::ArrayRef<WalkEvent> events) {
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

bool PatternReplay::isVisibleHere(Value value) const {
    Region* const here = _builder.getInsertionBlock()->getParent();
    return value.getParentRegion()->isAncestor(here);
}

void PatternReplay::crossJunction() {
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

Value PatternReplay::materializeJoin(const JoinConjunct& join) {
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

void PatternReplay::carried(llvm::SmallVectorImpl<Value>& columns, Value walkedFrom) const {
    for (const Value origin : _inFlight) {
        if (origin != walkedFrom) {
            columns.push_back(_current.lookup(origin));
        }
    }
}

void PatternReplay::carried(llvm::SmallVectorImpl<Value>& columns, llvm::SmallVectorImpl<Type>& types, Value walkedFrom) const {
    const size_t first = columns.size();
    carried(columns, walkedFrom);
    for (size_t index = first; index < columns.size(); index++) {
        types.push_back(columns[index].getType());
    }
}

void PatternReplay::rebindCarried(Operation* op, size_t firstCarried, Value walkedFrom) {
    size_t resultIndex = firstCarried;
    for (const Value origin : _inFlight) {
        if (origin != walkedFrom) {
            _current[origin] = op->getResult(resultIndex++);
        }
    }
}

void PatternReplay::keepRows(Value mask) {
    llvm::SmallVector<Value> columns;
    llvm::SmallVector<Type> types;
    carried(columns, types, Value());

    FilterOp filter = _builder.create<FilterOp>(_loc, types, mask, columns);
    rebindCarried(filter.getOperation(), 0, Value());
}

void PatternReplay::applyRootLabels() {
    ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(_pattern._root);
    if (!scanByLabel) {
        return;
    }

    const Value root = _current.lookup(_pattern._root->getResult(0));
    keepRows(checkLabels(root, scanByLabel.getLabels(), _loc, _builder));
}

void PatternReplay::bindHop(const PatternStep& step, Operation* walked, Value walkedFrom, size_t fromResult, size_t toResult) {
    rebindCarried(walked, hopFixedResultCount, walkedFrom);

    bind(step._from, walked->getResult(fromResult));
    bind(step._to, walked->getResult(toResult));
    bind(step._op->getResult(1), walked->getResult(1));
    bind(step._op->getResult(2), walked->getResult(2));
}

void PatternReplay::walkForward(const PatternStep& step) {
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

void PatternReplay::walkBackward(const PatternStep& step) {
    llvm::SmallVector<Value> columns;
    carried(columns, step._to);

    const Value from = _current.lookup(step._to);
    Operation* const walked = createReversedHop(step._op, from, ArrayAttr(), columns, _builder);

    bindHop(step, walked, step._to, hopReachedResult(walked), hopInputResult(walked));
}

void PatternReplay::walkExploration(ExplorePaths exploration, const PatternStep& step) {
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
