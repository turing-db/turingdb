#include "DBPasses.h"

#include <algorithm>
#include <memory>

#include "mlir/IR/Builders.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallBitVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "DBColumnElement.h"
#include "DBOps.h"
#include "DBCrossProducts.h"
#include "DBFilterUtils.h"
#include "DBLookupPredicates.h"
#include "DBPassUtils.h"
#include "DBPassWorklist.h"
#include "DBPatternWalk.h"
#include "DBRowCone.h"
#include "DBRowEstimator.h"
#include "DBUnwindEqualityCross.h"
#include "DBWrittenOrder.h"

namespace mlir::db {

#define GEN_PASS_DEF_REROOTPATTERNATSEED
#define GEN_PASS_DEF_REROOTPATTERNATEDGEIDS
#define GEN_PASS_DEF_REROOTPATTERNATCHEAPESTNODE
#include "DBPasses.h.inc"

namespace {

struct PatternSeed {
    PatternJunction _junction;
    SeedPattern _pattern;
    FilterOp _filter {nullptr};
    EqOp _equality {nullptr};
    Value _seeded;
    Value _ids;
    const PatternStep* _fetchedStep {nullptr};
    llvm::SmallVector<WalkEvent> _events;
};

// The junction column each column of a block is carried from through the filters after it,
// kept so the filters of one chain trace it once rather than once per filter. A reroot
// rewrites the block, so clear() follows every one.
class JunctionTraces {
public:
    OpResult trace(Value column, Block* block) {
        llvm::SmallVector<Value> climbed;
        Value junction;

        for (;;) {
            const OpResult result = dyn_cast<OpResult>(column);
            if (!result || result.getOwner()->getBlock() != block) {
                break;
            }

            const auto tracedIt = _junctions.find(column);
            if (tracedIt != _junctions.end()) {
                junction = tracedIt->second;
                break;
            }

            climbed.push_back(column);

            Operation* const op = result.getOwner();
            FilterOp filter = dyn_cast<FilterOp>(op);
            if (isa<CrossProduct, Unwind>(op)) {
                junction = column;
                break;
            } else if (!filter) {
                break;
            }

            column = filter.getColumnsToFilter()[result.getResultNumber()];
        }

        for (const Value value : climbed) {
            _junctions[value] = junction;
        }

        return junction ? cast<OpResult>(junction) : nullptr;
    }

    void clear() {
        _junctions.clear();
    }

private:
    llvm::DenseMap<Value, Value> _junctions;
};

bool matchSeedJunction(OpResult seededResult, PatternJunction& junction, SeedPattern& pattern) {
    Operation* const op = seededResult.getOwner();

    if (CrossProduct product = dyn_cast<CrossProduct>(op)) {
        const size_t leftCount = factorYield(op, 0).getNumOperands();
        const bool patternOnTheLeft = seededResult.getResultNumber() < leftCount;
        readProductJunction(product, patternOnTheLeft, junction);

        Region& patternFactor = patternOnTheLeft ? product.getLeftFactor() : product.getRightFactor();

        const size_t outputIndex = llvm::find(junction._patternResults, Value(seededResult)) - junction._patternResults.begin();
        Operation* const root = findPatternRoot(junction._patternOutputs[outputIndex]);
        if (!root || root->getParentRegion() != &patternFactor) {
            return false;
        }

        Block& block = patternFactor.front();
        llvm::SmallVector<Operation*> orderedOps;
        for (Operation& patternOp : llvm::make_range(block.begin(), Block::iterator(block.getTerminator()))) {
            pattern._ops.insert(&patternOp);
            orderedOps.push_back(&patternOp);
        }

        pattern._root = root;
        return analyzePattern(pattern, orderedOps);
    }

    Unwind unwind = cast<Unwind>(op);
    if (seededResult.getResultNumber() == 0) {
        return false;
    }

    readUnwindJunction(unwind, junction);

    Operation* const root = findPatternRoot(junction._patternOutputs[seededResult.getResultNumber() - 1]);
    if (!root || root->getBlock() != op->getBlock()) {
        return false;
    }

    collectPatternOps(root, op, pattern);

    llvm::SmallVector<Operation*> orderedOps;
    for (Operation& patternOp : llvm::make_range(Block::iterator(root), Block::iterator(op))) {
        if (pattern._ops.contains(&patternOp)) {
            orderedOps.push_back(&patternOp);
        }
    }

    if (!unwindsOverThePatternAlone(junction, pattern, orderedOps)) {
        return false;
    }

    return analyzePattern(pattern, orderedOps);
}

// @param visited holds the values an earlier call reached without finding the seed, so a
// caller asking of every filter along a chain walks the chain once
bool readsTheSeed(Value value, const PatternJunction& junction, llvm::DenseSet<Value>& visited) {
    llvm::SmallVector<Value> worklist {value};

    while (!worklist.empty()) {
        const Value current = worklist.pop_back_val();
        if (!visited.insert(current).second) {
            continue;
        }

        if (llvm::is_contained(junction._otherResults, current)) {
            return true;
        }

        Operation* const def = current.getDefiningOp();
        if (!standsFromTheJunction(def, junction)) {
            continue;
        }

        if (FilterOp carrying = dyn_cast<FilterOp>(def)) {
            const size_t resultIndex = cast<OpResult>(current).getResultNumber();
            worklist.push_back(carrying.getColumnsToFilter()[resultIndex]);
        } else {
            llvm::append_range(worklist, def->getOperands());
        }
    }

    return false;
}

bool isComputedFromTheSeed(Value ids, const PatternJunction& junction, Operation* filter) {
    llvm::SmallVector<Value> worklist {ids};
    llvm::DenseSet<Value> visited;
    llvm::DenseSet<Value> masksVisited;

    while (!worklist.empty()) {
        const Value current = worklist.pop_back_val();
        if (!visited.insert(current).second || llvm::is_contained(junction._otherResults, current)) {
            continue;
        }

        Operation* const def = current.getDefiningOp();
        if (!standsFromTheJunction(def, junction)) {
            continue;
        }

        if (def == junction._op || !def->isBeforeInBlock(filter)) {
            return false;
        }

        if (FilterOp carrying = dyn_cast<FilterOp>(def)) {
            if (readsTheSeed(carrying.getMask(), junction, masksVisited)) {
                return false;
            }

            const size_t resultIndex = cast<OpResult>(current).getResultNumber();
            worklist.push_back(carrying.getColumnsToFilter()[resultIndex]);
        } else if (!computesPerRow(def)) {
            return false;
        } else {
            llvm::append_range(worklist, def->getOperands());
        }
    }

    return true;
}

// The junction's rows reach the equality's filter through filters and row-wise ops alone,
// which keep their meaning over the rows the equality would have kept
bool readsTheJunctionRowWise(const PatternJunction& junction, FilterOp filter) {
    llvm::SmallPtrSet<Operation*, 16> readers;
    Operation* const junctionOp = junction._op;

    for (Operation& op : llvm::make_range(std::next(Block::iterator(junctionOp)), Block::iterator(filter.getOperation()))) {
        const bool readsTheRows = llvm::any_of(op.getOperands(), [&readers, junctionOp](Value operand) {
            Operation* const def = operand.getDefiningOp();
            return def == junctionOp || (def && readers.contains(def));
        });

        if (!readsTheRows) {
            continue;
        }

        if (!isa<FilterOp>(op) && !computesPerRow(&op)) {
            return false;
        }

        readers.insert(&op);
    }

    readers.insert(junctionOp);
    for (Operation* const reader : readers) {
        for (Operation* const user : reader->getUsers()) {
            if (user != filter.getOperation() && !readers.contains(user)) {
                return false;
            }
        }
    }

    return true;
}

// The values db.fetch_edges reads an edge out of: an edge, an integer, or a type-erased cell
bool namesEdgesByID(Value ids) {
    const Type element = ::db::declaredColumnElement(ids);

    return element.isSignlessInteger(64) || isa<storage::EdgeIDType, storage::AnyType>(element);
}

bool matchPatternSeedSide(FilterOp filter, EqOp equality, Value seeded, Value ids, JunctionTraces& traces, PatternSeed& seed) {
    const bool seedsAnEdge = isEdgeColumn(seeded) && namesEdgesByID(ids);
    if (!isNodeColumn(seeded) && !seedsAnEdge) {
        return false;
    }

    Block* const block = filter->getBlock();
    const OpResult seededResult = traces.trace(seeded, block);
    if (!seededResult) {
        return false;
    }

    // GCC 13 reads moving an empty event list into the seed as a memmove past its end
    // (-Warray-bounds), so the seed is rebuilt in place rather than assigned a fresh one
    std::destroy_at(&seed);
    std::construct_at(&seed);

    if (!matchSeedJunction(seededResult, seed._junction, seed._pattern)) {
        return false;
    }

    const PatternJunction& junction = seed._junction;
    const size_t outputIndex = llvm::find(junction._patternResults, Value(seededResult)) - junction._patternResults.begin();
    seed._seeded = patternOrigin(junction._patternOutputs[outputIndex], seed._pattern);
    seed._fetchedStep = seedsAnEdge ? findEdgeStep(seed._pattern, seed._seeded) : nullptr;

    const bool seedsAPatternEntity = seedsAnEdge ? static_cast<bool>(seed._fetchedStep) : isPatternNode(seed._pattern, seed._seeded);
    const bool walkableFromTheSeed = seedsAPatternEntity && isWalkableFrom(seed._pattern, seed._seeded, seed._fetchedStep);
    if (!walkableFromTheSeed) {
        return false;
    }

    const bool idsOverTheSeedRows = isComputedFromTheSeed(ids, junction, filter) && readsTheJunctionRowWise(junction, filter);
    if (!idsOverTheSeedRows) {
        return false;
    }

    // A literal list equated to the root or to an edge folds into an ID disjunction over the
    // pattern, which the scans by ID turn into a const scan listing the nodes or the edges at
    // plan time
    UnwindEqualityCross unwindEquality;
    CrossProduct product = dyn_cast<CrossProduct>(junction._op);
    const bool seedsTheRoot = seed._seeded == seed._pattern._root->getResult(0);
    const bool listsTheSeeds = seed._fetchedStep || seedsTheRoot;
    const bool foldsIntoAConstScan = product && listsTheSeeds && matchUnwindEqualityCross(product, unwindEquality);
    if (foldsIntoAConstScan) {
        return false;
    }

    const llvm::SmallPtrSet<Operation*, 1> noneApplied;
    if (!planWalk(seed._pattern, seed._seeded, seed._fetchedStep, noneApplied, false, seed._events)) {
        return false;
    }

    seed._filter = filter;
    seed._equality = equality;
    seed._ids = ids;

    return true;
}

bool matchPatternSeed(FilterOp filter, JunctionTraces& traces, PatternSeed& seed) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    for (const Value conjunct : conjuncts) {
        EqOp equality = conjunct.getDefiningOp<EqOp>();
        if (!isEqualityOfTheFilter(equality, filter)) {
            continue;
        }

        const Value lhs = equality.getLhs();
        const Value rhs = equality.getRhs();

        if (matchPatternSeedSide(filter, equality, lhs, rhs, traces, seed) || matchPatternSeedSide(filter, equality, rhs, lhs, traces, seed)) {
            return true;
        }
    }

    return false;
}

Value cloneSeedIDs(Value ids, const PatternJunction& junction, mlir::IRMapping& seedColumns, mlir::OpBuilder& builder) {
    llvm::SmallVector<Value> worklist {ids};

    while (!worklist.empty()) {
        const Value value = worklist.back();
        if (seedColumns.contains(value)) {
            worklist.pop_back();
            continue;
        }

        Operation* const def = value.getDefiningOp();
        if (!standsFromTheJunction(def, junction)) {
            seedColumns.map(value, value);
            worklist.pop_back();
            continue;
        }

        const size_t resultIndex = cast<OpResult>(value).getResultNumber();

        if (FilterOp carrying = dyn_cast<FilterOp>(def)) {
            const Value carried = carrying.getColumnsToFilter()[resultIndex];
            if (seedColumns.contains(carried)) {
                seedColumns.map(value, seedColumns.lookup(carried));
                worklist.pop_back();
            } else {
                worklist.push_back(carried);
            }

            continue;
        }

        bool operandsPending = false;
        for (const Value operand : llvm::reverse(def->getOperands())) {
            if (!seedColumns.contains(operand)) {
                worklist.push_back(operand);
                operandsPending = true;
            }
        }

        if (operandsPending) {
            continue;
        }

        Operation* const cloned = builder.clone(*def, seedColumns);
        seedColumns.map(value, cloned->getResult(resultIndex));
        worklist.pop_back();
    }

    return seedColumns.lookup(ids);
}

// Whether every row of a column names a node the graph holds, so that a fetch would keep
// it as it is: a scan's nodes, a fetch's, or either end of a hop or a walk
bool namesGraphNodes(Value column) {
    for (;;) {
        const OpResult result = dyn_cast<OpResult>(column);
        if (!result) {
            return false;
        }

        Operation* const op = result.getOwner();
        const size_t resultIndex = result.getResultNumber();

        if (isa<ScanNodes, ScanNodesByLabel, ScanNodesByPropertyValue>(op)) {
            return true;
        } else if (FilterOp filter = dyn_cast<FilterOp>(op)) {
            column = filter.getColumnsToFilter()[resultIndex];
        } else if (isa<FetchNodes>(op) && resultIndex > 0) {
            column = op->getOperand(resultIndex);
        } else if (isa<FetchNodes>(op)) {
            return true;
        } else if (isa<FetchEdges>(op) && resultIndex >= hopFixedResultCount) {
            column = op->getOperand(1 + resultIndex - hopFixedResultCount);
        } else if (isa<FetchEdges>(op)) {
            return resultIndex == 0 || resultIndex == EDGE_TARGETS_RESULT;
        } else if (isEdgeHop(op) && resultIndex >= hopFixedResultCount) {
            column = op->getOperand(1 + resultIndex - hopFixedResultCount);
        } else if (isEdgeHop(op)) {
            return resultIndex == hopInputResult(op) || resultIndex == hopReachedResult(op);
        } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op); exploration && resultIndex >= pathFixedResultCount) {
            column = exploration.getColumnsToFilter()[resultIndex - pathFixedResultCount];
        } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op)) {
            return column == exploration.getSrcids() || column == exploration.getTgtids();
        } else {
            return false;
        }
    }
}

void reroot(PatternSeed& seed, mlir::RewriterBase& rewriter) {
    PatternJunction& junction = seed._junction;
    Operation* const junctionOp = junction._op;
    const Location loc = junctionOp->getLoc();

    rewriter.setInsertionPoint(junctionOp);

    // The seed's rows on their own: the other factor's body, or the list unwound again
    // without the pattern's columns
    llvm::SmallVector<Value> seedColumns;
    if (junction._otherFactor) {
        const Operation::operand_range yielded = factorYieldColumns(*junction._otherFactor);
        seedColumns.assign(yielded.begin(), yielded.end());

        Block& seedBlock = junction._otherFactor->front();
        junctionOp->getBlock()->getOperations().splice(Block::iterator(junctionOp),
                                                       seedBlock.getOperations(),
                                                       seedBlock.begin(),
                                                       Block::iterator(seedBlock.getTerminator()));
    } else {
        Unwind unwind = cast<Unwind>(junctionOp);
        const llvm::SmallVector<Type> types {unwind.getElement().getType()};
        Unwind seedUnwind = rewriter.create<Unwind>(loc, types, unwind.getSource(), ValueRange());
        seedColumns.push_back(seedUnwind.getElement());
    }

    mlir::IRMapping seedMapping;
    for (size_t index = 0; index < seedColumns.size(); index++) {
        seedMapping.map(junction._otherResults[index], seedColumns[index]);
    }

    const Value ids = cloneSeedIDs(seed._ids, junction, seedMapping, rewriter);
    const PatternStep* const fetchedStep = seed._fetchedStep;

    llvm::SmallVector<Value> seededColumns {ids};
    size_t seededCount = 1;
    if (fetchedStep) {
        Operation* const hop = fetchedStep->_op;

        llvm::SmallVector<Type> fetchTypes;
        for (size_t index = 0; index < hopFixedResultCount; index++) {
            fetchTypes.push_back(hop->getResult(index).getType());
        }

        for (const Value column : seedColumns) {
            fetchTypes.push_back(column.getType());
        }

        FetchEdges fetch = rewriter.create<FetchEdges>(loc, fetchTypes, ids, seedColumns, isa<GetEdges>(hop));
        seededColumns.assign(fetch->getResults().begin(), fetch->getResults().end());
        seededCount = hopFixedResultCount;
    } else if (namesGraphNodes(ids)) {
        llvm::append_range(seededColumns, seedColumns);
    } else {
        llvm::SmallVector<Type> fetchTypes {seed._seeded.getType()};
        for (const Value column : seedColumns) {
            fetchTypes.push_back(column.getType());
        }

        FetchNodes fetch = rewriter.create<FetchNodes>(loc, fetchTypes, ids, seedColumns);
        seededColumns.assign(fetch->getResults().begin(), fetch->getResults().end());
    }

    PatternReplay replay(seed._pattern, rewriter, loc);
    for (size_t index = 0; index < seedColumns.size(); index++) {
        replay.bind(junction._otherResults[index], seededColumns[seededCount + index]);
    }

    if (fetchedStep) {
        Operation* const hop = fetchedStep->_op;

        replay.bind(fetchedStep->_from, seededColumns[hopInputResult(hop)]);
        replay.bind(fetchedStep->_to, seededColumns[hopReachedResult(hop)]);
        replay.bind(hop->getResult(EDGE_IDS_RESULT), seededColumns[EDGE_IDS_RESULT]);
        replay.bind(hop->getResult(EDGE_TYPES_RESULT), seededColumns[EDGE_TYPES_RESULT]);
    } else {
        replay.bind(seed._seeded, seededColumns.front());
    }

    replay.walk(seed._events);

    for (size_t index = 0; index < junction._patternResults.size(); index++) {
        junction._patternResults[index].replaceAllUsesWith(replay.materialize(junction._patternOutputs[index]));
    }

    for (Value seedResult : junction._otherResults) {
        seedResult.replaceAllUsesWith(replay.currentOf(seedResult));
    }

    dropConjunct(seed._filter, seed._equality.getResult(), rewriter);

    if (junction._otherFactor) {
        rewriter.eraseOp(junctionOp);
        return;
    }

    llvm::SmallVector<Operation*> patternOps;
    for (Operation& op : llvm::make_range(Block::iterator(seed._pattern._root), Block::iterator(junctionOp))) {
        if (seed._pattern._ops.contains(&op)) {
            patternOps.push_back(&op);
        }
    }

    rewriter.eraseOp(junctionOp);
    for (Operation* const op : llvm::reverse(patternOps)) {
        op->dropAllUses();
        rewriter.eraseOp(op);
    }
}

struct RerootPatternAtSeed : public impl::RerootPatternAtSeedBase<RerootPatternAtSeed> {
    void runOnOperation() override {
        JunctionTraces traces;

        const auto matchSeed = [&traces](FilterOp filter, PatternSeed& seed) {
            return matchPatternSeed(filter, traces, seed);
        };

        const auto rerootAtSeed = [&traces](PatternSeed& seed, mlir::RewriterBase& rewriter) {
            traces.clear();
            reroot(seed, rewriter);
        };

        runFilterWorklist<PatternSeed>(getOperation(), matchSeed, rerootAtSeed);
    }
};

// A pattern one of whose steps binds the edges a disjunction of ID tests reads after the
// pattern: a hop, rebuilt from the edges the disjunction lists and walked out from both their
// ends, or a walk starting on them, rebuilt from the ends the listed edges leave. The walk
// leaves those ends over other edges too, so the disjunction still tests its rows.
struct ListedEdgesPattern {
    SeedPattern _pattern;
    FilterOp _filter {nullptr};
    const PatternStep* _listedStep {nullptr};
    EntityIDList _edgeIDs;
    llvm::SmallVector<Operation*> _patternOps;
    llvm::SmallVector<WalkEvent> _events;
};

// Nothing but the pattern and the filter reads what the pattern binds, and the filter carries
// the pattern's columns alone, so the rows rebuilt for the filter replace them all
bool endsAtTheFilter(const SeedPattern& pattern, llvm::ArrayRef<Operation*> patternOps, FilterOp filter) {
    for (Operation* const patternOp : patternOps) {
        for (Operation* const user : patternOp->getUsers()) {
            if (user != filter.getOperation() && !pattern._ops.contains(user)) {
                return false;
            }
        }
    }

    return llvm::all_of(filter.getColumnsToFilter(), [&pattern](Value column) {
        Operation* const def = column.getDefiningOp();
        return def && pattern._ops.contains(def);
    });
}

const PatternStep* findStep(const SeedPattern& pattern, Operation* op) {
    for (const PatternStep& step : pattern._steps) {
        if (step._op == op) {
            return &step;
        }
    }

    return nullptr;
}

bool matchListedEdgesPattern(FilterOp filter, ListedEdgesPattern& match) {
    const Value mask = filter.getMask();
    const Value column = disjunctionColumn(mask);
    if (!column) {
        return false;
    }

    WalkEndEdge walkStart;
    const bool listsHopEdges = isEdgeColumn(column);
    const bool listsWalkStarts = !listsHopEdges && matchWalkEndEdge(column, walkStart) && walkStart._first;
    if (!listsHopEdges && !listsWalkStarts) {
        return false;
    }

    Operation* const root = findPatternRoot(listsHopEdges ? column : walkStart._walk.getSrcids());
    if (!root || root->getBlock() != filter->getBlock()) {
        return false;
    }

    SeedPattern& pattern = match._pattern;
    collectPatternOps(root, filter, pattern);

    for (Operation& op : llvm::make_range(Block::iterator(root), Block::iterator(filter.getOperation()))) {
        if (pattern._ops.contains(&op)) {
            match._patternOps.push_back(&op);
        }
    }

    if (!endsAtTheFilter(pattern, match._patternOps, filter) || !analyzePattern(pattern, match._patternOps)) {
        return false;
    }

    const PatternStep* const step = listsHopEdges ? findEdgeStep(pattern, patternOrigin(column, pattern)) : findStep(pattern, walkStart._walk.getOperation());
    if (!step || !collectIDDisjunction(mask, column, match._edgeIDs)) {
        return false;
    }

    const PatternStep* const fetchedStep = listsHopEdges ? step : nullptr;
    const Value seeded = listsHopEdges ? step->_op->getResult(EDGE_IDS_RESULT) : step->_from;
    const llvm::SmallPtrSet<Operation*, 1> noneApplied;
    if (!isWalkableFrom(pattern, seeded, fetchedStep) || !planWalk(pattern, seeded, fetchedStep, noneApplied, false, match._events)) {
        return false;
    }

    if (listsWalkStarts) {
        match._events.push_back(WalkEvent {._kind = WalkEvent::Kind::Filter, ._filter = filter});
    }

    match._filter = filter;
    match._listedStep = step;

    return true;
}

// Every row rebuilt from a hop's listed edges holds the disjunction, so the filter testing it
// goes. A walk rebuilt from their ends tests it again.
void rerootAtListedEdges(ListedEdgesPattern& match, mlir::RewriterBase& rewriter) {
    FilterOp filter = match._filter;
    const Location loc = filter.getLoc();
    const PatternStep& step = *match._listedStep;

    rewriter.setInsertionPoint(filter);
    PatternReplay replay(match._pattern, rewriter, loc);

    if (ExplorePaths walk = dyn_cast<ExplorePaths>(step._op)) {
        const Value starts = scanListedEdgeEnds(match._edgeIDs, walk.getStepDirection(0), false, loc, rewriter);
        RemoveDuplicates seeds = rewriter.create<RemoveDuplicates>(loc, TypeRange {starts.getType()}, ValueRange {starts}, Value());
        replay.bind(step._from, seeds.getResult(0));
    } else {
        Operation* const hop = step._op;
        ConstScanEdges listed = scanListedEdges(match._edgeIDs, isa<GetEdges>(hop), loc, rewriter);

        replay.bind(step._from, listed->getResult(hopInputResult(hop)));
        replay.bind(step._to, listed->getResult(hopReachedResult(hop)));
        replay.bind(hop->getResult(EDGE_IDS_RESULT), listed.getEids());
        replay.bind(hop->getResult(EDGE_TYPES_RESULT), listed.getEtypes());
    }

    replay.walk(match._events);

    const ResultRange filtered = filter.getFilteredColumns();
    const Operation::operand_range carried = filter.getColumnsToFilter();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(replay.materialize(carried[index]));
    }

    const MaskCone cone = collectMaskCone(filter.getMask());
    rewriter.eraseOp(filter);

    for (Operation* const op : llvm::reverse(match._patternOps)) {
        op->dropAllUses();
        rewriter.eraseOp(op);
    }

    for (Operation* const coneOp : llvm::reverse(cone._ops)) {
        if (!match._pattern._ops.contains(coneOp)) {
            eraseIfUnused(coneOp, rewriter);
        }
    }
}

struct RerootPatternAtEdgeIDs : public impl::RerootPatternAtEdgeIDsBase<RerootPatternAtEdgeIDs> {
    void runOnOperation() override {
        runFilterWorklist<ListedEdgesPattern>(getOperation(), matchListedEdgesPattern, rerootAtListedEdges);
    }
};

// The estimate counts rows and edges read rather than time, and a walk off the written root
// reads the nodes after its first hop out of their order, so a pattern moves off the root
// the query wrote only when another start costs this much less
constexpr double REROOT_GAIN_FACTOR = 1.3;

// A pattern rooted at a scan up to the first op reading it that is not rebuilt with it, and
// the walk out of the node making the fewest rows, scanned by the labels its filters ask for.
// A walk crossing the other side of its junction in partway rebuilds the junction there.
struct CheapestRoot {
    SeedPattern _pattern;
    llvm::SmallVector<Operation*> _orderedOps;
    Operation* _end {nullptr};
    Value _start;
    ArrayAttr _labels;
    llvm::SmallVector<WalkEvent> _events;
    JunctionJoins _joins;
    bool _crossesJunction {false};
};

// False when a graph write stands before the end, which the rebuilt pattern would read past
bool collectRootedPattern(Operation* scan, CheapestRoot& match) {
    SeedPattern& pattern = match._pattern;
    pattern._root = scan;
    pattern._ops.insert(scan);
    match._orderedOps.push_back(scan);

    const auto definedByThePattern = [&pattern](Value value) {
        Operation* const def = value.getDefiningOp();
        return def && pattern._ops.contains(def);
    };

    for (Operation& op : llvm::make_range(std::next(Block::iterator(scan)), scan->getBlock()->end())) {
        bool writesTheGraph = false;
        bool readsThePattern = false;
        op.walk([&](Operation* nested) {
            writesTheGraph = writesTheGraph || isGraphWrite(nested);
            readsThePattern = readsThePattern || llvm::any_of(nested->getOperands(), definedByThePattern);
        });

        if (writesTheGraph) {
            return false;
        } else if (!readsThePattern) {
            continue;
        }

        ExplorePaths exploration = dyn_cast<ExplorePaths>(op);
        const bool explores = exploration && walksFreely(exploration);

        const bool buildsThePattern = isa<FilterOp>(op) || isHop(&op) || explores || computesPerRow(&op);
        if (!buildsThePattern) {
            match._end = &op;
            return true;
        }

        FilterOp filter = dyn_cast<FilterOp>(op);
        const bool carriesRows = filter || isHop(&op) || explores;
        const OperandRange carried = filter ? filter.getColumnsToFilter() : op.getOperands();
        if (carriesRows && !llvm::all_of(carried, definedByThePattern)) {
            return false;
        }

        pattern._ops.insert(&op);
        match._orderedOps.push_back(&op);
    }

    return false;
}

void collectWrittenWalk(const CheapestRoot& match, llvm::SmallVectorImpl<WalkEvent>& events) {
    const llvm::SmallVector<PatternStep>& steps = match._pattern._steps;

    size_t stepIndex = 0;
    for (Operation* const op : match._orderedOps) {
        if (FilterOp filter = dyn_cast<FilterOp>(op)) {
            events.push_back(WalkEvent {._kind = WalkEvent::Kind::Filter, ._filter = filter});
        } else if (isHop(op) || isa<ExplorePaths>(op)) {
            events.push_back(WalkEvent {._kind = WalkEvent::Kind::Forward, ._step = &steps[stepIndex++]});
        }
    }
}

bool isComputedAhead(Value value, Operation* op) {
    Operation* const ancestor = value.getParentBlock()->findAncestorOpInBlock(*op);
    if (!ancestor) {
        return false;
    }

    Operation* const def = value.getDefiningOp();

    return !def || def->isBeforeInBlock(ancestor);
}

// The keys fuse_hash_join joins on: each side of the equality computed from one column of a
// side of the junction of its own
void readJoinKeys(Value conjunct, const PatternJunction& junction, bool usesHashJoin, JoinConjunct& join) {
    EqOp equality = conjunct.getDefiningOp<EqOp>();
    if (!equality) {
        return;
    }

    bool keysThePattern = false;
    bool keysTheOtherSide = false;
    bool hashesKeys = true;
    for (const Value key : {equality.getLhs(), equality.getRhs()}) {
        JoinKeySide side;
        side._key = key;
        collectRowCone({key}, equality->getBlock(), side._cone);

        if (side._cone._inputs.size() != 1) {
            return;
        }

        const Value column = climbFilters(side._cone._inputs.front());
        keysThePattern = keysThePattern || llvm::is_contained(junction._patternResults, column);
        keysTheOtherSide = keysTheOtherSide || llvm::is_contained(junction._otherResults, column);
        hashesKeys = hashesKeys && isHashableKey(side);
    }

    join._equatesKeys = keysThePattern && keysTheOtherSide;
    join._hashes = join._equatesKeys && hashesKeys && usesHashJoin && isa<CrossProduct>(junction._op);
}

// A conjunct reading both sides of the junction. Its cone is cloned where the junction
// stands, so it reads nothing computed after the junction.
bool matchJoinConjunct(Value conjunct,
                       FilterOp filter,
                       const PatternJunction& junction,
                       const SeedPattern& pattern,
                       bool usesHashJoin,
                       JoinConjunct& join) {
    join._conjunct = conjunct;
    join._filter = filter;
    collectRowCone({conjunct}, filter->getBlock(), join._cone);

    bool readsTheOtherSide = false;
    for (const Value input : join._cone._inputs) {
        const Value climbed = climbFilters(input);

        if (llvm::is_contained(junction._otherResults, climbed)) {
            readsTheOtherSide = true;
        } else if (const Value patternColumn = junctionSource(junction, climbed)) {
            collectPatternInputs(patternColumn, pattern, join._patternInputs);
        } else if (!isComputedAhead(input, junction._op)) {
            return false;
        }
    }

    readJoinKeys(conjunct, junction, usesHashJoin, join);

    return readsTheOtherSide && !join._patternInputs.empty();
}

bool matchJunctionJoins(const CheapestRoot& match,
                        const RowEstimator& estimator,
                        const DBPassContext& context,
                        JunctionJoins& joins) {
    const SeedPattern& pattern = match._pattern;
    PatternJunction& junction = joins._junction;

    Yield yield = dyn_cast<Yield>(match._end);
    CrossProduct product = yield ? dyn_cast<CrossProduct>(yield->getParentOp()) : CrossProduct();
    Unwind unwind = dyn_cast<Unwind>(match._end);

    if (product) {
        const bool holdsThePatternAlone = llvm::all_of(yield->getBlock()->without_terminator(), [&pattern](Operation& op) {
            return pattern._ops.contains(&op) || computesFromTheRow(&op);
        });

        if (!holdsThePatternAlone) {
            return false;
        }

        readProductJunction(product, yield->getParentRegion() == &product.getLeftFactor(), junction);
        joins._crossedRows = estimateFactorRows(*junction._otherFactor, estimator);
    } else if (unwind && !unwind.getColumnsToFilter().empty()) {
        readUnwindJunction(unwind, junction);
        if (!unwindsOverThePatternAlone(junction, pattern, match._orderedOps)) {
            return false;
        }

        joins._crossedRows = estimateUnwoundRows(unwind);
    } else {
        return false;
    }

    for (const Value output : junction._patternOutputs) {
        Operation* const def = output.getDefiningOp();
        if (!def || !pattern._ops.contains(def)) {
            return false;
        }
    }

    llvm::SmallVector<std::pair<Value, FilterOp>, 4> conjuncts;
    FilterStack stack(junction._op);
    for (FilterOp filter = stack.getReader(); filter && stack.isExclusiveTo(filter); filter = stack.getReader()) {
        stack.climbTo(filter);

        llvm::SmallVector<Value, 4> filterConjuncts;
        collectConjuncts(filter.getMask(), filterConjuncts);
        for (const Value conjunct : filterConjuncts) {
            conjuncts.emplace_back(conjunct, filter);
        }
    }

    // Equalities go first, so the filter right over a rebuilt junction is one fuse_hash_join
    // can fuse with it into a hash join
    std::stable_partition(conjuncts.begin(), conjuncts.end(), [](const std::pair<Value, FilterOp>& conjunct) {
        return static_cast<bool>(conjunct.first.getDefiningOp<EqOp>());
    });

    for (const auto& [conjunct, filter] : conjuncts) {
        JoinConjunct& join = joins._joins.emplace_back();
        if (!matchJoinConjunct(conjunct, filter, junction, pattern, context._usesHashJoin, join)) {
            joins._joins.pop_back();
        }
    }

    return !joins._joins.empty();
}

bool isStepEvent(const WalkEvent& event) {
    return event._kind == WalkEvent::Kind::Forward || event._kind == WalkEvent::Kind::Backward;
}

// The walk crossing the junction's other side in ahead of its event @param position, after
// which each join conjunct runs before the next step once the steps have bound what it reads
void crossWalkAt(const JunctionJoins& joins,
                 Value start,
                 llvm::ArrayRef<WalkEvent> walk,
                 size_t position,
                 llvm::SmallVectorImpl<WalkEvent>& events) {
    llvm::DenseSet<Value> bound {start};
    llvm::SmallBitVector joined(joins._joins.size());

    const auto joinBound = [&joins, &bound, &joined, &events]() {
        for (size_t index = 0; index < joins._joins.size(); index++) {
            const JoinConjunct& join = joins._joins[index];
            const bool readsBound = llvm::all_of(join._patternInputs, [&bound](void* input) {
                return bound.contains(Value::getFromOpaquePointer(input));
            });

            if (!joined.test(index) && readsBound) {
                events.push_back(WalkEvent {._kind = WalkEvent::Kind::Join, ._join = &join});
                joined.set(index);
            }
        }
    };

    bool crossed = false;
    for (size_t index = 0; index < walk.size(); index++) {
        const WalkEvent& event = walk[index];
        const bool isStep = isStepEvent(event);

        if (index == position) {
            events.push_back(WalkEvent {._kind = WalkEvent::Kind::Cross});
            crossed = true;
            joinBound();
        } else if (crossed && isStep) {
            joinBound();
        }

        events.push_back(event);

        if (isStep) {
            bindStep(*event._step, bound);
        }
    }

    if (!crossed) {
        events.push_back(WalkEvent {._kind = WalkEvent::Kind::Cross});
    }

    joinBound();
}

void collectScanLabelFilters(const SeedPattern& pattern,
                             Value node,
                             llvm::SmallPtrSetImpl<Operation*>& absorbed,
                             llvm::SmallVectorImpl<Attribute>& labels) {
    for (FilterOp filter : pattern._filters) {
        CheckLabelConstraint check = filter.getMask().getDefiningOp<CheckLabelConstraint>();
        const ArrayAttr conjunction = check ? check.getConjunction() : ArrayAttr();
        if (!conjunction) {
            continue;
        }

        GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
        if (!labelSet || patternOrigin(labelSet.getInputNodes(), pattern) != node) {
            continue;
        }

        absorbed.insert(filter.getOperation());
        for (const Attribute label : conjunction) {
            if (!llvm::is_contained(labels, label)) {
                labels.push_back(label);
            }
        }
    }
}

// A walk crossing the other side in once nothing but join conjuncts is left leaves the
// junction and the filters over it as they stand
void settleCross(CheapestRoot& match) {
    llvm::SmallVector<WalkEvent>& events = match._events;
    const auto cross = llvm::find_if(events, [](const WalkEvent& event) {
        return event._kind == WalkEvent::Kind::Cross;
    });

    const bool joinsAlone = std::all_of(std::next(cross), events.end(), [](const WalkEvent& event) {
        return event._kind == WalkEvent::Kind::Join;
    });

    if (joinsAlone) {
        events.erase(cross, events.end());
    } else {
        match._crossesJunction = true;
    }
}

bool matchCheapestRoot(Operation* scan, const RowEstimator& estimator, const DBPassContext& context, CheapestRoot& match) {
    SeedPattern& pattern = match._pattern;
    if (!collectRootedPattern(scan, match) || !analyzePattern(pattern, match._orderedOps) || pattern._steps.empty()) {
        return false;
    }

    const JunctionJoins* const joins = matchJunctionJoins(match, estimator, context, match._joins) ? &match._joins : nullptr;

    const Value root = scan->getResult(0);
    ScanNodesByLabel rootScan = dyn_cast<ScanNodesByLabel>(scan);
    const ArrayAttr rootLabels = rootScan ? rootScan.getLabels() : ArrayAttr();

    llvm::SmallVector<WalkEvent> written;
    collectWrittenWalk(match, written);

    llvm::SmallVector<WalkEvent> crossedWritten;
    if (joins) {
        crossWalkAt(*joins, root, written, written.size(), crossedWritten);
    }

    const llvm::ArrayRef<WalkEvent> writtenWalk = joins ? crossedWritten : written;

    double cheapest = estimateWalkCost(pattern, root, rootLabels, writtenWalk, joins, estimator) / REROOT_GAIN_FACTOR;
    bool foundCheaper = false;

    const auto considerWalk = [&](Value candidate, ArrayAttr startLabels, llvm::ArrayRef<WalkEvent> events) {
        const double cost = estimateWalkCost(pattern, candidate, startLabels, events, joins, estimator);
        if (cost < cheapest) {
            cheapest = cost;
            foundCheaper = true;

            match._start = candidate;
            match._labels = startLabels;
            match._events.assign(events.begin(), events.end());
        }
    };

    llvm::SmallVector<Value> candidates {root};
    for (const PatternStep& step : pattern._steps) {
        candidates.push_back(step._to);
    }

    for (const Value candidate : candidates) {
        if (!isWalkableFrom(pattern, candidate, nullptr)) {
            continue;
        }

        const bool isTheRoot = candidate == root;

        llvm::SmallVector<Attribute> labels;
        if (isTheRoot && rootLabels) {
            labels.append(rootLabels.begin(), rootLabels.end());
        }

        llvm::SmallPtrSet<Operation*, 4> absorbed;
        collectScanLabelFilters(pattern, candidate, absorbed, labels);
        const ArrayAttr startLabels = labels.empty() ? ArrayAttr() : ArrayAttr::get(scan->getContext(), labels);

        llvm::SmallVector<WalkEvent> events;
        if (!planWalk(pattern, candidate, nullptr, absorbed, isTheRoot || !rootLabels, events)) {
            continue;
        }

        if (joins) {
            // Crossed in only ahead of a step or at the end, the filters a scan or a hop is
            // followed by stay beside it, where the passes fusing them into it look for them
            for (size_t position = 0; position <= events.size(); position++) {
                const bool beforeAStepOrTheEnd = position == events.size() || isStepEvent(events[position]);
                if (!beforeAStepOrTheEnd) {
                    continue;
                }

                llvm::SmallVector<WalkEvent> crossedEvents;
                crossWalkAt(*joins, candidate, events, position, crossedEvents);
                considerWalk(candidate, startLabels, crossedEvents);
            }
        } else {
            considerWalk(candidate, startLabels, events);
        }
    }

    if (foundCheaper && joins) {
        settleCross(match);
    }

    return foundCheaper;
}

void rerootAtCheapestNode(CheapestRoot& match, mlir::RewriterBase& rewriter) {
    const SeedPattern& pattern = match._pattern;
    Operation* const rootOp = pattern._root;
    const Location loc = rootOp->getLoc();
    const Type nodesType = rootOp->getResult(0).getType();

    rewriter.setInsertionPoint(match._end);

    Value scanned;
    if (match._labels) {
        scanned = rewriter.create<ScanNodesByLabel>(loc, nodesType, match._labels).getResult();
    } else {
        scanned = rewriter.create<ScanNodes>(loc, nodesType).getResult();
    }

    PatternReplay replay(pattern, rewriter, loc);
    if (match._crossesJunction) {
        replay.setJunction(&match._joins._junction);
    }

    replay.bind(match._start, scanned);
    replay.walk(match._events);

    if (match._crossesJunction) {
        const JunctionJoins& joins = match._joins;
        Operation* const junctionOp = joins._junction._op;

        for (const Value result : junctionOp->getResults()) {
            rewriter.replaceAllUsesWith(result, replay.materializeJunctionResult(result));
        }

        for (const JoinConjunct& join : joins._joins) {
            dropConjunct(join._filter, join._conjunct, rewriter);
        }

        rewriter.eraseOp(junctionOp);
    } else {
        const auto readOutsideThePattern = [&pattern](OpOperand& use) {
            return !pattern._ops.contains(use.getOwner());
        };

        for (Operation* const op : match._orderedOps) {
            for (Value result : op->getResults()) {
                if (llvm::any_of(result.getUses(), readOutsideThePattern)) {
                    result.replaceUsesWithIf(replay.materialize(result), readOutsideThePattern);
                }
            }
        }
    }

    for (Operation* const op : llvm::reverse(match._orderedOps)) {
        op->dropAllUses();
        rewriter.eraseOp(op);
    }
}

struct RerootPatternAtCheapestNode : public impl::RerootPatternAtCheapestNodeBase<RerootPatternAtCheapestNode> {
    RerootPatternAtCheapestNode() {}

    RerootPatternAtCheapestNode(const DBPassContext* context)
        : _context(context)
    {
    }

    void runOnOperation() override {
        if (!_context->_view) {
            return;
        }

        const RowEstimator estimator(*_context->_view);

        const DBPassContext& context = *_context;
        runWorklist<ScanNodes, ScanNodesByLabel>(getOperation(), [&estimator, &context](Operation* op, mlir::RewriterBase& rewriter) {
            CheapestRoot match;
            if (!matchCheapestRoot(op, estimator, context, match)) {
                return false;
            }

            rerootAtCheapestNode(match, rewriter);
            return true;
        });
    }

private:
    const DBPassContext* _context {&defaultPassContext};
};

}

std::unique_ptr<Pass> createRerootPatternAtCheapestNode(const DBPassContext* context) {
    return std::make_unique<RerootPatternAtCheapestNode>(context);
}

}
