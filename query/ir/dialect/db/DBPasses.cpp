#include "DBPasses.h"

#include <stdint.h>
#include <algorithm>
#include <limits>
#include <memory>
#include <optional>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Transforms/RegionUtils.h"
#include "mlir/Transforms/GreedyPatternRewriteDriver.h"

#include "llvm/ADT/BitVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SetVector.h"
#include "llvm/ADT/SmallBitVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringSet.h"

#include "CardinalityEstimation.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "views/GraphView.h"

#include "IRConstantColumn.h"
#include "PropertyScanLiteral.h"
#include "DBOps.h"

#include "BioAssert.h"

namespace mlir::db {

#define GEN_PASS_DEF_FUSESCANBYLABEL
#define GEN_PASS_DEF_PUSHDOWNFILTERS
#define GEN_PASS_DEF_FUSEUNWINDEQUALITY
#define GEN_PASS_DEF_PLACEINFACTORS
#define GEN_PASS_DEF_FUSESCANBYNODEIDS
#define GEN_PASS_DEF_FUSESCANBYPROPERTYVALUE
#define GEN_PASS_DEF_FUSESCANEDGES
#define GEN_PASS_DEF_FUSEEDGESBYTYPE
#define GEN_PASS_DEF_FUSESCANEDGESBYTYPE
#define GEN_PASS_DEF_FUSELABELPREDICATES
#define GEN_PASS_DEF_FUSEEDGETYPEPREDICATES
#define GEN_PASS_DEF_NARROWEDGETYPEREADS
#define GEN_PASS_DEF_FUSESCANOUTEDGESBYLABEL
#define GEN_PASS_DEF_FUSESCANINEDGESBYLABEL
#define GEN_PASS_DEF_FUSESCANEDGESBYENDPOINTLABEL
#define GEN_PASS_DEF_FUSEEDGESBYENDPOINTLABEL
#define GEN_PASS_DEF_REMOVEREDUNDANTLABELCHECKS
#define GEN_PASS_DEF_REUSEPROPERTYREADS
#define GEN_PASS_DEF_FUSEHASHJOIN
#define GEN_PASS_DEF_FUSEFETCHNODES
#define GEN_PASS_DEF_REROOTPATTERNATSEED
#define GEN_PASS_DEF_FUSEEXPLOREENDCONSTRAINT
#define GEN_PASS_DEF_FUSEEXPLOREHOPLABELS
#define GEN_PASS_DEF_SINKMAKEPATH
#define GEN_PASS_DEF_FUSEPATHELEMENTS
#define GEN_PASS_DEF_FUSEEXPLOREENDNODES
#define GEN_PASS_DEF_FUSEEXPLOREENDFACTOR
#define GEN_PASS_DEF_FUSEEXPLOREENDSET
#define GEN_PASS_DEF_FUSEEXPLORELISTPREDICATE
#define GEN_PASS_DEF_FUSEEXPLOREDISTINCTENDS
#define GEN_PASS_DEF_COUNTPATHROWS
#define GEN_PASS_DEF_TRIMUNREADCOLUMNS
#define GEN_PASS_DEF_COUNTFROMMETADATA
#include "DBPasses.h.inc"

namespace {

const DBPassContext defaultPassContext;

struct LabelScanChain {
    ScanNodes scan;
    GetNodeLabelSet labelSet;
    CheckLabelConstraint check;
    ArrayAttr labels;
};

bool matchLabelScanChain(FilterOp filter, LabelScanChain& chain) {
    const Operation::operand_range columns = filter.getColumnsToFilter();
    if (columns.size() != 1) {
        return false;
    }

    const Value scanColumn = columns[0];

    chain.check = filter.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!chain.check) {
        return false;
    }

    chain.labels = chain.check.getConjunction();
    if (!chain.labels) {
        return false;
    }

    chain.labelSet = chain.check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!chain.labelSet || chain.labelSet.getInputNodes() != scanColumn) {
        return false;
    }

    chain.scan = scanColumn.getDefiningOp<ScanNodes>();
    if (!chain.scan) {
        return false;
    }

    return true;
}

void eraseIfUnused(Operation* op) {
    if (op && op->use_empty()) {
        op->erase();
    }
}

// The driver every filter pass shares: collect the filters first, since rewriting erases
// ops and would invalidate the walk, then rewrite each one that matches.
template <typename Match, typename MatchFilter, typename RewriteFilter>
void runFilterPass(Operation* root, MatchFilter matchFilter, RewriteFilter rewriteFilter, mlir::OpBuilder& builder) {
    llvm::SmallVector<FilterOp> filters;
    root->walk([&](FilterOp filter) {
        filters.push_back(filter);
    });

    for (FilterOp filter : filters) {
        Match match;
        if (!matchFilter(filter, match)) {
            continue;
        }

        rewriteFilter(filter, match, builder);
    }
}

// The ops of one kind a pass has left to try. It listens to the rewriter, so it queues every
// such op a rewrite creates and drops every op a rewrite erases.
template <typename... OpTypes>
class OpWorklist : public mlir::RewriterBase::Listener {
public:
    explicit OpWorklist(Operation* root) {
        root->walk([this](Operation* op) {
            if (isa<OpTypes...>(op)) {
                push(op);
            }
        });
    }

    Operation* pop() {
        while (_next < _ops.size()) {
            Operation* const op = _ops[_next++];
            if (op) {
                _positions.erase(op);
                return op;
            }
        }

        return nullptr;
    }

    void notifyOperationInserted(Operation* op, mlir::OpBuilder::InsertPoint previous) override {
        if (isa<OpTypes...>(op)) {
            push(op);
        }
    }

    void notifyOperationErased(Operation* op) override {
        const auto found = _positions.find(op);
        if (found != _positions.end()) {
            _ops[found->second] = nullptr;
            _positions.erase(found);
        }
    }

private:
    std::vector<Operation*> _ops;
    llvm::DenseMap<Operation*, size_t> _positions;
    size_t _next {0};

    void push(Operation* op) {
        if (_positions.try_emplace(op, _ops.size()).second) {
            _ops.push_back(op);
        }
    }
};

// A match can read IR far above its op, so a rewrite can make an op it never touched match.
// The worklist runs again until a round rewrites nothing, as MLIR's greedy driver does.
template <typename... OpTypes, typename RewriteOp>
void runWorklist(Operation* root, RewriteOp rewriteOp) {
    bool rewritten = true;
    while (rewritten) {
        rewritten = false;

        OpWorklist<OpTypes...> worklist(root);
        mlir::IRRewriter rewriter(root->getContext(), &worklist);

        while (Operation* op = worklist.pop()) {
            if (rewriteOp(op, rewriter)) {
                rewritten = true;
            }
        }
    }
}

template <typename Match, typename MatchFilter, typename RewriteFilter>
void runFilterWorklist(Operation* root, MatchFilter matchFilter, RewriteFilter rewriteFilter) {
    runWorklist<FilterOp>(root, [&matchFilter, &rewriteFilter](Operation* op, mlir::RewriterBase& rewriter) {
        Match match;
        if (!matchFilter(cast<FilterOp>(op), match)) {
            return false;
        }

        rewriteFilter(match, rewriter);
        return true;
    });
}

void fuseScanByLabel(FilterOp filter, const LabelScanChain& chain, mlir::OpBuilder& builder) {
    ScanNodes scan = chain.scan;
    GetNodeLabelSet labelSet = chain.labelSet;
    CheckLabelConstraint check = chain.check;

    builder.setInsertionPoint(filter);
    ScanNodesByLabel scanByLabel = builder.create<ScanNodesByLabel>(filter.getLoc(),
                                                                    scan.getResult().getType(),
                                                                    chain.labels);

    Operation* const filterOp = filter.getOperation();
    filterOp->getResult(0).replaceAllUsesWith(scanByLabel.getResult());
    filterOp->erase();

    // Drop the now-dead chain, consumer to producer, each only if unused.
    eraseIfUnused(check);
    eraseIfUnused(labelSet);
    eraseIfUnused(scan);
}

struct FuseScanByLabel : public impl::FuseScanByLabelBase<FuseScanByLabel> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<LabelScanChain>(getOperation(), matchLabelScanChain, fuseScanByLabel, builder);
    }
};

bool isNodeSource(Operation* op) {
    return isa<ScanNodes, ScanNodesByLabel, ConstScanNodes, ScanNodesByPropertyValue>(op);
}

bool isEdgeHop(Operation* op) {
    return isa<GetOutEdges,
               GetInEdges,
               GetEdges,
               GetOutEdgesByType,
               GetInEdgesByType,
               GetOutEdgesByLabel,
               GetInEdgesByLabel,
               GetOutEdgesByTypeAndLabel,
               GetInEdgesByTypeAndLabel>(op);
}

bool isReverseHop(Operation* op) {
    return isa<GetInEdges, GetInEdgesByType, GetInEdgesByLabel, GetInEdgesByTypeAndLabel>(op);
}

// The result of a hop holding the node it walks from, and the one holding the node it reaches
size_t hopInputResult(Operation* hop) {
    return isReverseHop(hop) ? 3 : 0;
}

size_t hopReachedResult(Operation* hop) {
    return isReverseHop(hop) ? 0 : 3;
}

constexpr size_t hopFixedResultCount = 4;

// srcids, tgtids and paths; the carry set follows
constexpr size_t pathFixedResultCount = 3;

// The columns a cross_product factor yields, in result order.
Operation::operand_range factorYieldColumns(mlir::Region& factor) {
    mlir::Block& factorBlock = factor.front();
    Yield yield = cast<Yield>(factorBlock.getTerminator());

    return yield.getColumns();
}

Value productFactorColumn(CrossProduct product, size_t resultIndex) {
    const Operation::operand_range leftColumns = factorYieldColumns(product.getLeftFactor());
    if (resultIndex < leftColumns.size()) {
        return leftColumns[resultIndex];
    }

    return factorYieldColumns(product.getRightFactor())[resultIndex - leftColumns.size()];
}

using ColumnEquality = std::pair<Value, Value>;

void collectConjuncts(Value mask, llvm::SmallVectorImpl<Value>& conjuncts);

bool isNodeColumn(Value column) {
    const ColumnType type = dyn_cast<ColumnType>(column.getType());
    return type && isa<storage::NodeIDType>(type.getType());
}

// Every equality of two node columns the filter requires: it keeps only the rows where they
// agree, so below it anything reading one of them reads the other just as well. A filter
// whose mask is a conjunction requires each of its conjuncts, equalities among them.
void collectColumnEqualities(FilterOp filter, llvm::SmallVectorImpl<ColumnEquality>& equalities) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    for (const Value conjunct : conjuncts) {
        EqOp equality = conjunct.getDefiningOp<EqOp>();
        if (!equality || !isNodeColumn(equality.getLhs()) || !isNodeColumn(equality.getRhs())) {
            continue;
        }

        equalities.push_back(ColumnEquality {equality.getLhs(), equality.getRhs()});
    }
}

// One step of the climb from a column to the column its variable was bound at. It crosses a
// producer where the op builds the rows - a hop or a cross product - rather than dropping them
struct LineageStep {
    Value _next;
    bool _isAnchor {false};
    bool _crossesProducer {false};
};

// Sets _isAnchor where @param column is where its variable was bound, _next to the column
// the climb goes on from, and neither where the climb cannot pass the op defining it
void stepTowardLineageAnchor(Value column, LineageStep& step) {
    Operation* const def = column.getDefiningOp();
    if (!def) {
        return;
    }

    const size_t resultIndex = cast<OpResult>(column).getResultNumber();

    if (isNodeSource(def)) {
        step._isAnchor = true;
    } else if (FilterOp filter = dyn_cast<FilterOp>(def)) {
        step._next = filter.getColumnsToFilter()[resultIndex];
    } else if (CrossProduct product = dyn_cast<CrossProduct>(def)) {
        step._next = productFactorColumn(product, resultIndex);
        step._crossesProducer = true;
    } else if (isEdgeHop(def)) {
        // The input node re-surfaces as srcids (forward) or tgtids (reverse) and each
        // carried column passes through: those continue a variable that existed before
        // the hop, so keep climbing. The opposite node end and the eids/etypes are
        // bound here, so the variable is born at this hop - its earliest filter point.
        if (resultIndex == hopInputResult(def)) {
            step._next = def->getOperand(0);
            step._crossesProducer = true;
        } else if (resultIndex >= hopFixedResultCount) {
            // Carried columns follow input_nodes (operand 0) in operand order.
            step._next = def->getOperand(1 + (resultIndex - hopFixedResultCount));
            step._crossesProducer = true;
        } else {
            step._isAnchor = true;
        }
    } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(def)) {
        constexpr size_t tgtResultIndex = 1;

        // The seed re-surfaces as srcids and each carried column passes through; the end
        // node and the path are born here, whichever direction the exploration walks -
        // except where the walk is one that comes back to its seed, which leaves the end
        // holding that seed row for row, so a predicate on it is one on the seed.
        const bool endHoldsTheSeed = resultIndex == tgtResultIndex && exploration.getEndsOnSeed();

        if (resultIndex == 0 || endHoldsTheSeed) {
            step._next = def->getOperand(0);
            step._crossesProducer = true;
        } else if (resultIndex >= pathFixedResultCount) {
            step._next = def->getOperand(1 + (resultIndex - pathFixedResultCount));
            step._crossesProducer = true;
        } else {
            step._isAnchor = true;
        }
    } else if (Unwind unwind = dyn_cast<Unwind>(def)) {
        // The element is born at the unwind, and each carried column repeats the rows it
        // carries once per element
        if (resultIndex == 0) {
            step._isAnchor = true;
        } else {
            step._next = unwind.getColumnsToFilter()[resultIndex - 1];
            step._crossesProducer = true;
        }
    }
}

// The climbs of one pass run, each built from the climb one step up. A push_down_filters push
// moves a single-variable predicate, which changes no anchor and crosses no producer, so a
// climb stays true across pushes; its equalities are kept as their sides' anchors, which a
// push never erases. A rewrite that replaces an anchor has to clear() them.
class LineageClimbs {
public:
    Value anchorOf(Value column) {
        computeClimb(column);
        return _climbs.at(column)._anchor;
    }

    Value climb(Value column, bool& crossedProducer, llvm::SmallVectorImpl<ColumnEquality>& anchorEqualities) {
        computeClimb(column);

        const Climb& climb = _climbs.at(column);
        crossedProducer = crossedProducer || climb._crossedProducer;

        if (climb._equalities != NO_LIST) {
            llvm::append_range(anchorEqualities, _equalityLists[climb._equalities]);
        }

        return climb._anchor;
    }

    void forget(Operation* op) {
        for (const Value result : op->getResults()) {
            _climbs.erase(result);
        }
    }

    void clear() {
        _climbs.clear();
        _equalityLists.clear();
    }

private:
    static constexpr size_t NO_LIST = SIZE_MAX;

    struct Climb {
        Value _anchor;
        bool _crossedProducer {false};
        size_t _equalities {NO_LIST};
    };

    // A climb's distinct equalities, nearest filter first. A filter whose equalities the climb
    // one step up already holds shares that list, so a chain repeating one equality keeps one.
    llvm::DenseMap<Value, Climb> _climbs;
    std::vector<llvm::SmallVector<ColumnEquality, 2>> _equalityLists;

    void computeClimb(Value column) {
        llvm::SmallVector<Value> worklist {column};
        llvm::SmallVector<ColumnEquality, 4> filterEqualities;

        while (!worklist.empty()) {
            const Value value = worklist.back();
            if (_climbs.contains(value)) {
                worklist.pop_back();
                continue;
            }

            LineageStep step;
            stepTowardLineageAnchor(value, step);

            filterEqualities.clear();
            if (FilterOp filter = value.getDefiningOp<FilterOp>()) {
                collectColumnEqualities(filter, filterEqualities);
            }

            bool dependenciesPending = false;
            const auto requireClimb = [this, &worklist, &dependenciesPending](Value dependency) {
                if (dependency && !_climbs.contains(dependency)) {
                    worklist.push_back(dependency);
                    dependenciesPending = true;
                }
            };

            requireClimb(step._next);
            for (const ColumnEquality& equality : filterEqualities) {
                requireClimb(equality.first);
                requireClimb(equality.second);
            }

            if (dependenciesPending) {
                continue;
            }

            Climb climb;
            if (step._isAnchor) {
                climb._anchor = value;
            } else if (step._next) {
                const Climb next = _climbs.at(step._next);
                climb._anchor = next._anchor;
                climb._crossedProducer = next._crossedProducer || step._crossesProducer;
                climb._equalities = next._equalities;

                for (const ColumnEquality& equality : llvm::reverse(filterEqualities)) {
                    const ColumnEquality anchors {_climbs.at(equality.first)._anchor, _climbs.at(equality.second)._anchor};
                    const bool relatesTwoAnchors = anchors.first && anchors.second && anchors.first != anchors.second;
                    if (!relatesTwoAnchors) {
                        continue;
                    }

                    llvm::SmallVector<ColumnEquality, 2> equalities {anchors};
                    if (climb._equalities != NO_LIST) {
                        const llvm::SmallVector<ColumnEquality, 2>& held = _equalityLists[climb._equalities];
                        if (llvm::is_contained(held, anchors)) {
                            continue;
                        }

                        llvm::append_range(equalities, held);
                    }

                    _equalityLists.push_back(equalities);
                    climb._equalities = _equalityLists.size() - 1;
                }
            }

            _climbs[value] = climb;
            worklist.pop_back();
        }
    }
};

bool isMaskComputeOp(Operation* op) {
    return isa<EqOp, NeqOp, GtOp, LtOp, GteOp, LteOp,
               StartsWithOp, EndsWithOp, ContainsOp, InOp,
               AndOp, OrOp, XorOp, NotOp,
               AddOp, SubOp, MulOp, DivOp, ModOp, PowOp, ConcatOp,
               ConstantOp,
               GetNodeProperties, GetEdgeProperties>(op);
}

struct MaskCone {
    llvm::SmallVector<Operation*> _ops;
    llvm::SmallSetVector<Value, 4> _inputs;
};

// One suspended operand loop of the cone walk: the op whose operands are being
// visited, and how many of them are done.
struct ConeFrame {
    Operation* _op {nullptr};
    unsigned _nextOperand {0};
};

// A cone is what a pass moves onto rows the query may not have run it on, so an op raising
// on some row's value stays out of it, as one of the cone's inputs
bool raisesOnSomeRows(Operation* op) {
    mlir::ConditionallySpeculatable speculation = dyn_cast<mlir::ConditionallySpeculatable>(op);

    return speculation && speculation.getSpeculatability() == mlir::Speculation::NotSpeculatable;
}

// Gathers the mask's compute cone and the external columns feeding it. Valid
// even when the cone spans blocks.
void collectConePostOrder(Value mask,
                          llvm::function_ref<bool(Operation*)> inCone,
                          llvm::SmallPtrSet<Operation*, 8>& visited,
                          MaskCone& cone) {
    const auto joinsTheCone = [inCone](Operation* op) {
        return inCone(op) && !raisesOnSomeRows(op);
    };

    Operation* const maskDef = mask.getDefiningOp();

    if (!maskDef || !joinsTheCone(maskDef)) {
        cone._inputs.insert(mask);
        return;
    }

    // A caller walking several conjuncts into one cone shares the set across the
    // calls, so a root already collected by an earlier conjunct is dropped here.
    if (!visited.insert(maskDef).second) {
        return;
    }

    llvm::SmallVector<ConeFrame> stack {ConeFrame {maskDef, 0}};
    while (!stack.empty()) {
        ConeFrame& frame = stack.back();

        if (frame._nextOperand == frame._op->getNumOperands()) {
            cone._ops.push_back(frame._op);
            stack.pop_back();
            continue;
        }

        const Value operand = frame._op->getOperand(frame._nextOperand);
        frame._nextOperand++;

        Operation* const operandDef = operand.getDefiningOp();
        if (!operandDef || !joinsTheCone(operandDef)) {
            cone._inputs.insert(operand);
            continue;
        }

        // A second path can only reach an op through a value that something else
        // also reads, so a singly-used result needs no membership check. An OR
        // chain is singly-used throughout and never touches the set at all.
        const bool reachableOnce = operandDef->getNumResults() == 1 && operand.hasOneUse();
        if (reachableOnce || visited.insert(operandDef).second) {
            stack.push_back(ConeFrame {operandDef, 0});
        }
    }
}

MaskCone collectMaskCone(Value mask) {
    MaskCone cone;

    llvm::SmallPtrSet<Operation*, 8> visited;
    collectConePostOrder(mask, isMaskComputeOp, visited, cone);

    return cone;
}

// A single-variable property-predicate filter and where its mask should be rebuilt:
// the lineage anchor of the one column its mask reads.
struct PushablePredicate {
    Value _anchor;
    MaskCone _cone;
};

// The anchor a predicate can be rebuilt on instead of @param anchor: the equalities the climb
// crossed hold whole classes of columns to the same node, and where one of the class was
// scanned for, the predicate reads that node off the scan. Applying it there is what spares
// the walk between the two. The class is closed over the equalities rather than read a pair
// at a time, so a column held to the scan through an intermediate is still found.
Value scannedAnchorEqualTo(Value anchor, llvm::ArrayRef<ColumnEquality> anchorEqualities) {
    if (!anchor || isNodeSource(anchor.getDefiningOp())) {
        return {};
    }

    llvm::SmallVector<Value, 4> anchorClass {anchor};
    llvm::SmallPtrSet<void*, 4> seen {anchor.getAsOpaquePointer()};

    for (size_t index = 0; index < anchorClass.size(); index++) {
        for (const ColumnEquality& equality : anchorEqualities) {
            const Value lhs = equality.first;
            const Value rhs = equality.second;
            const Value held = lhs == anchorClass[index] ? rhs : (rhs == anchorClass[index] ? lhs : Value {});
            if (!held || !seen.insert(held.getAsOpaquePointer()).second) {
                continue;
            }

            if (isNodeSource(held.getDefiningOp())) {
                return held;
            }

            anchorClass.push_back(held);
        }
    }

    return {};
}

bool matchPushablePredicate(FilterOp filter, LineageClimbs& climbs, PushablePredicate& pushable) {
    Operation* const maskDef = filter.getMask().getDefiningOp();
    if (!maskDef || !isMaskComputeOp(maskDef)) {
        return false;
    }

    pushable._cone = collectMaskCone(filter.getMask());
    if (pushable._cone._inputs.empty()) {
        return false;
    }

    bool crossedProducer = false;
    llvm::SmallVector<ColumnEquality, 4> anchorEqualities;
    for (const Value input : pushable._cone._inputs) {
        const Value inputAnchor = climbs.climb(input, crossedProducer, anchorEqualities);
        if (!inputAnchor) {
            return false;
        }

        if (!pushable._anchor) {
            pushable._anchor = inputAnchor;
        } else if (pushable._anchor != inputAnchor) {
            // More than one lineage feeds the mask: not a single-variable predicate.
            return false;
        }
    }

    const Value equatedAnchor = scannedAnchorEqualTo(pushable._anchor, anchorEqualities);
    if (equatedAnchor) {
        pushable._anchor = equatedAnchor;
        crossedProducer = true;
    }

    // Crossing only filters leaves the predicate over the rows the anchor produced already:
    // moving it would swap two filters over the same rows, or step between a constraint
    // filter and the op that is about to absorb it.
    return crossedProducer;
}

void bypassFilter(FilterOp filter) {
    const ResultRange filtered = filter.getFilteredColumns();
    const Operation::operand_range carried = filter.getColumnsToFilter();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(carried[index]);
    }
}

void pushDownPredicate(FilterOp filter, const PushablePredicate& pushable, mlir::OpBuilder& builder) {
    Value anchor = pushable._anchor;
    const MaskCone& cone = pushable._cone;
    const mlir::Location loc = filter.getLoc();

    Operation* const anchorProducer = anchor.getDefiningOp();

    llvm::SmallVector<mlir::Value> liveColumns;
    for (const mlir::Value result : anchorProducer->getResults()) {
        if (!result.use_empty()) {
            liveColumns.push_back(result);
        }
    }

    // Rebuild the mask over the anchor version of the column, right after it is bound.
    mlir::IRMapping mapping;
    for (const Value input : cone._inputs) {
        mapping.map(input, anchor);
    }

    builder.setInsertionPointAfter(anchorProducer);

    llvm::SmallPtrSet<Operation*, 8> boundaryReaders;
    for (Operation* const coneOp : cone._ops) {
        Operation* const cloned = builder.clone(*coneOp, mapping);
        boundaryReaders.insert(cloned);
        builder.setInsertionPointAfter(cloned);
    }

    const Value clonedMask = mapping.lookup(filter.getMask());

    llvm::SmallVector<mlir::Type, 8> resultTypes;
    for (const mlir::Value column : liveColumns) {
        resultTypes.push_back(column.getType());
    }

    FilterOp pushed = builder.create<FilterOp>(loc, resultTypes, clonedMask, liveColumns);
    boundaryReaders.insert(pushed.getOperation());

    // Replace live columns with filtered versions
    for (size_t i {0}; mlir::Value liveCol : liveColumns) {
        liveCol.replaceAllUsesExcept(pushed.getResult(i++), boundaryReaders);
    }

    // Remove original filter
    bypassFilter(filter);
    filter.erase();

    for (Operation* const coneOp : llvm::reverse(cone._ops)) {
        eraseIfUnused(coneOp);
    }
}

struct PushDownFilters : public impl::PushDownFiltersBase<PushDownFilters> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<FilterOp> filters;
        getOperation()->walk([&filters](FilterOp filter) {
            filters.push_back(filter);
        });

        LineageClimbs climbs;
        for (FilterOp filter : filters) {
            PushablePredicate pushable;
            if (!matchPushablePredicate(filter, climbs, pushable)) {
                continue;
            }

            climbs.forget(filter.getOperation());
            for (Operation* const coneOp : pushable._cone._ops) {
                climbs.forget(coneOp);
            }

            pushDownPredicate(filter, pushable, builder);
        }
    }
};

struct ScanSource {
    Operation* _op {nullptr};
    ArrayAttr _labels;
    Value _column;
};

// Any carried column beyond the scan's own (a property read before the WHERE, say) has no
// filtered counterpart in a source op to be rewired to, so such a filter has to stay.
bool matchSoleScanSource(FilterOp filter, ScanSource& source) {
    const Operation::operand_range columns = filter.getColumnsToFilter();
    if (columns.size() != 1) {
        return false;
    }

    source._column = columns.front();
    source._op = source._column.getDefiningOp();
    if (!source._op || !isa<ScanNodes, ScanNodesByLabel>(source._op)) {
        return false;
    }

    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(source._op)) {
        source._labels = scanByLabel.getLabels();
    }

    return true;
}

Value checkLabels(Value nodes, ArrayAttr labels, Location loc, mlir::OpBuilder& builder) {
    MLIRContext* const context = builder.getContext();
    const Type labelSetType = ColumnType::get(context, storage::LabelSetIDType::get(context));
    const Type boolType = ColumnType::get(context, storage::BoolType::get(context));

    GetNodeLabelSet labelSet = builder.create<GetNodeLabelSet>(loc, labelSetType, nodes);
    CheckLabelConstraint check = builder.create<CheckLabelConstraint>(loc,
                                                                      boolType,
                                                                      labelSet.getResult(),
                                                                      builder.getArrayAttr({labels}));

    return check.getResult();
}

// Cuts the columns to the rows whose first column holds a node carrying every label
void keepLabelledRows(llvm::SmallVectorImpl<Value>& columns, ArrayAttr labels, Location loc, mlir::OpBuilder& builder) {
    const Value mask = checkLabels(columns.front(), labels, loc, builder);

    const ValueRange columnRange(columns);
    FilterOp labelFilter = builder.create<FilterOp>(loc, columnRange.getTypes(), mask, columnRange);

    columns.assign(labelFilter.getResults().begin(), labelFilter.getResults().end());
}

void replaceFilterWithSource(FilterOp filter, Value fused, Operation* source, const MaskCone& cone) {
    for (Value filtered : filter.getFilteredColumns()) {
        filtered.replaceAllUsesWith(fused);
    }
    filter.erase();

    for (Operation* const coneOp : llvm::reverse(cone._ops)) {
        eraseIfUnused(coneOp);
    }
    eraseIfUnused(source);
}

bool matchNodeIDEquality(Value mask, Value scanColumn, int64_t& nodeID) {
    EqOp equality = mask.getDefiningOp<EqOp>();
    if (!equality) {
        return false;
    }

    const Value lhs = equality.getLhs();
    const Value rhs = equality.getRhs();

    Value constantSide;
    if (lhs == scanColumn) {
        constantSide = rhs;
    } else if (rhs == scanColumn) {
        constantSide = lhs;
    } else {
        return false;
    }

    ConstantOp constant = constantSide.getDefiningOp<ConstantOp>();
    if (!constant) {
        return false;
    }

    const IntegerAttr literal = dyn_cast<IntegerAttr>(constant.getValue());
    if (!literal || !literal.getType().isSignlessInteger(64)) {
        return false;
    }

    nodeID = literal.getInt();

    return nodeID >= 0;
}

// An UNWIND of N elements folds into a chain of N - 1 ORs, so the walk keeps its own stack:
// recursing once per OR overflows the thread's stack on a list of a few thousand.
bool collectNodeIDDisjunction(Value mask, Value scanColumn, llvm::SmallVectorImpl<int64_t>& nodeIDs) {
    llvm::SmallVector<Value> pending {mask};
    while (!pending.empty()) {
        const Value disjunct = pending.pop_back_val();

        if (OrOp disjunction = disjunct.getDefiningOp<OrOp>()) {
            pending.push_back(disjunction.getRhs());
            pending.push_back(disjunction.getLhs());
            continue;
        }

        int64_t nodeID = 0;
        if (!matchNodeIDEquality(disjunct, scanColumn, nodeID)) {
            return false;
        }

        nodeIDs.push_back(nodeID);
    }

    return true;
}

// The unwind factor of a cross product whose elements reach nothing but one equality
// against a column of the other factor, and the ops that equality is rebuilt from.
struct UnwindEqualityCross {
    CrossProduct _product {nullptr};
    UnwindConst _unwind {nullptr};
    Region* _relationFactor {nullptr};
    bool _unwindOnTheLeft {false};
    bool _readsTheIDs {false};
    Value _unwoundColumn;
    Value _comparedColumn;
    EqOp _equality {nullptr};
    FilterOp _filter {nullptr};
};

// The constant unwind a factor is nothing but, null for any other factor. Anything else
// in the region would be dropped along with it, so the unwind has to be all there is
// besides the yield of its one column.
UnwindConst matchUnwindFactor(Region& factor) {
    Block& block = factor.front();
    if (block.getOperations().size() != 2) {
        return nullptr;
    }

    Yield yield = dyn_cast<Yield>(block.getTerminator());
    if (!yield || yield.getColumns().size() != 1) {
        return nullptr;
    }

    return yield.getColumns().front().getDefiningOp<UnwindConst>();
}

// Whether one comparison per element stands in for what the unwound column is compared to.
// A heterogeneous list rides the type-erased column whose cells are compared by the tag
// each carries, which no set of typed comparisons reproduces, and a repeated element emits
// the rows it matches once per copy, which a membership test does not express.
bool elementsCompareDistinctly(UnwindConst unwind) {
    const ColumnType column = cast<ColumnType>(unwind.getResult().getType());
    if (isa<storage::ListElementType>(column.getType())) {
        return false;
    }

    llvm::SmallDenseSet<Attribute, 8> seen;
    for (const Attribute element : unwind.getElements()) {
        if (!isa<IntegerAttr, FloatAttr, StringAttr>(element)) {
            return false;
        }

        if (!seen.insert(element).second) {
            return false;
        }
    }

    return true;
}

// On every row the filter keeps, the elements hold the value of the column they were
// compared to. A node or an edge equals an integer by the ID it carries, so a projection
// reads the elements back through id() of an entity column, never through the entity.
bool comparesAnEntity(Value comparedColumn) {
    const ColumnType column = cast<ColumnType>(comparedColumn.getType());

    return isa<storage::NodeIDType, storage::EdgeIDType>(column.getType());
}

bool unwindsIntegers(UnwindConst unwind) {
    const ColumnType column = cast<ColumnType>(unwind.getResult().getType());

    return column.getType().isSignlessInteger(64);
}

// A count(*) reads how many rows a column has, never what it holds, so any column of the
// same rows stands in for the one it counts.
bool onlyCountsRows(Value column) {
    return llvm::all_of(column.getUsers(), [](Operation* user) {
        Count count = dyn_cast<Count>(user);
        return count && count.getRows();
    });
}

// A clause reading the rows through a region of its own spells the type of every column it
// takes in the block argument standing for it, and in the results it hands back, so a
// column of another type cannot be put in its place.
bool readThroughABlockArgument(Value column) {
    const auto takesItThroughARegion = [](Operation* user) {
        return isa<OptionalMatch, CallSubquery, ExistsSubquery, CountSubquery, PatternComprehension>(user);
    };

    return llvm::any_of(column.getUsers(), takesItThroughARegion);
}

// The one equality among the users of @param unwoundColumn, provided nothing else reads it
// but the filter that equality masks - so dropping the unwind leaves no other reader behind.
EqOp matchSoleEquality(Value unwoundColumn, FilterOp& filter) {
    EqOp equality;
    for (Operation* const user : unwoundColumn.getUsers()) {
        const EqOp candidate = dyn_cast<EqOp>(user);
        if (!candidate) {
            continue;
        }

        if (equality) {
            return nullptr;
        }

        equality = candidate;
    }

    if (!equality || !equality.getResult().hasOneUse()) {
        return nullptr;
    }

    filter = dyn_cast<FilterOp>(*equality.getResult().getUsers().begin());
    if (!filter || filter.getMask() != equality.getResult()) {
        return nullptr;
    }

    for (Operation* const user : unwoundColumn.getUsers()) {
        const bool testsTheColumn = user == equality.getOperation();
        const bool carriesTheColumn = user == filter.getOperation();

        if (!testsTheColumn && !carriesTheColumn) {
            return nullptr;
        }
    }

    return equality;
}

bool matchUnwindEqualityCross(CrossProduct product, UnwindEqualityCross& match) {
    Region& leftFactor = product.getLeftFactor();
    Region& rightFactor = product.getRightFactor();

    const UnwindConst leftUnwind = matchUnwindFactor(leftFactor);
    const UnwindConst rightUnwind = matchUnwindFactor(rightFactor);

    // Two unwind factors have no relation between them to fold either into.
    if (static_cast<bool>(leftUnwind) == static_cast<bool>(rightUnwind)) {
        return false;
    }

    match._product = product;
    match._unwind = leftUnwind ? leftUnwind : rightUnwind;
    match._relationFactor = leftUnwind ? &rightFactor : &leftFactor;
    match._unwindOnTheLeft = static_cast<bool>(leftUnwind);

    if (!elementsCompareDistinctly(match._unwind)) {
        return false;
    }

    // The results are the left factor's yielded columns followed by the right factor's, so
    // an unwind on the left contributes the first and one on the right the last.
    const size_t unwoundIndex = match._unwindOnTheLeft ? 0 : product.getResults().size() - 1;
    match._unwoundColumn = product.getResult(unwoundIndex);

    match._equality = matchSoleEquality(match._unwoundColumn, match._filter);
    if (!match._equality) {
        return false;
    }

    const Value lhs = match._equality.getLhs();
    match._comparedColumn = lhs == match._unwoundColumn ? match._equality.getRhs() : lhs;

    // The compared column has to be one the rows still carry once the product is gone:
    // a column the relation yields, or a value computed from one. A column defined inside
    // a factor is dropped with it.
    Operation* const comparedDef = match._comparedColumn.getDefiningOp();
    const bool survivesTheProduct = comparedDef
                                    && comparedDef->getParentRegion() == product->getParentRegion();

    if (!survivesTheProduct) {
        return false;
    }

    // Codegen carries every column in flight, so the filter holding the elements does not
    // mean anything reads them: an unused result is dropped rather than stood in for, and
    // only a column something does read has to be one the compared column can replace.
    const Operation::operand_range carried = match._filter.getColumnsToFilter();
    const ResultRange filtered = match._filter.getFilteredColumns();

    const bool entityCompared = comparesAnEntity(match._comparedColumn);

    size_t survivingColumns = 0;
    for (size_t index = 0; index < carried.size(); index++) {
        const bool holdsTheElements = carried[index] == match._unwoundColumn;

        if (holdsTheElements) {
            const Value elements = filtered[index];
            const bool readsTheIDs = entityCompared && !onlyCountsRows(elements);
            const Type standInType = readsTheIDs ? elements.getType() : match._comparedColumn.getType();
            const bool changesTheType = elements.getType() != standInType;

            if (elements.use_empty()) {
                continue;
            } else if (readsTheIDs && !unwindsIntegers(match._unwind)) {
                return false;
            } else if (changesTheType && readThroughABlockArgument(elements)) {
                return false;
            }

            match._readsTheIDs = match._readsTheIDs || readsTheIDs;
        }

        survivingColumns++;
    }

    // A filter of nothing cuts nothing, so the elements were all it carried and the rows
    // it kept are read from the relation directly.
    if (survivingColumns == 0) {
        return false;
    }

    return true;
}

// Moves the relation factor's ops out to where the product stood and rewires the results
// it contributed to the columns it yielded, leaving the product's own results unread.
void inlineRelationFactor(CrossProduct product, Region& relationFactor, bool unwindOnTheLeft) {
    Block& relationBlock = relationFactor.front();
    Yield relationYield = cast<Yield>(relationBlock.getTerminator());

    const llvm::SmallVector<Value> yielded(relationYield.getColumns().begin(),
                                           relationYield.getColumns().end());

    Block* const parentBlock = product->getBlock();
    parentBlock->getOperations().splice(Block::iterator(product),
                                        relationBlock.getOperations(),
                                        relationBlock.begin(),
                                        Block::iterator(relationYield));

    // The unwound column is the product's first result or its last, so the relation's run
    // from the other end.
    const size_t firstRelationResult = unwindOnTheLeft ? 1 : 0;
    for (size_t index = 0; index < yielded.size(); index++) {
        product.getResult(firstRelationResult + index).replaceAllUsesWith(yielded[index]);
    }
}

Value buildElementDisjunction(UnwindEqualityCross& match, mlir::OpBuilder& builder) {
    const Location loc = match._equality.getLoc();
    const Type boolColumnType = match._equality.getResult().getType();

    builder.setInsertionPoint(match._equality);

    Value mask;
    for (const Attribute element : match._unwind.getElements()) {
        ConstantOp constant = builder.create<ConstantOp>(loc, element);
        EqOp equality = builder.create<EqOp>(loc, boolColumnType, match._comparedColumn, constant.getResult());

        if (!mask) {
            mask = equality.getResult();
            continue;
        }

        mask = builder.create<OrOp>(loc, boolColumnType, mask, equality.getResult()).getResult();
    }

    return mask;
}

void fuseUnwindEquality(UnwindEqualityCross& match, mlir::OpBuilder& builder) {
    inlineRelationFactor(match._product, *match._relationFactor, match._unwindOnTheLeft);

    // Inlining rewired every use of the columns the relation contributed, the equality's
    // own operand among them, so the compared column is read back rather than remembered.
    const Value lhs = match._equality.getLhs();
    match._comparedColumn = lhs == match._unwoundColumn ? match._equality.getRhs() : lhs;

    const Value mask = buildElementDisjunction(match, builder);

    // The elements the rows carried are gone. On every row the filter keeps they were the
    // compared value, so that column takes their place where something still reads them,
    // and the slot goes away where nothing does.
    const Operation::operand_range carried = match._filter.getColumnsToFilter();
    const ResultRange filtered = match._filter.getFilteredColumns();

    llvm::SmallVector<Value> columns;
    llvm::SmallVector<Type> resultTypes;
    llvm::SmallVector<size_t> keptColumns;
    llvm::SmallVector<size_t> elementsReadAsIDs;
    for (size_t index = 0; index < carried.size(); index++) {
        const bool holdsTheElements = carried[index] == match._unwoundColumn;
        if (holdsTheElements && filtered[index].use_empty()) {
            continue;
        } else if (holdsTheElements && match._readsTheIDs) {
            elementsReadAsIDs.push_back(index);
            continue;
        }

        const Value kept = holdsTheElements ? match._comparedColumn : carried[index];

        columns.push_back(kept);
        resultTypes.push_back(kept.getType());
        keptColumns.push_back(index);
    }

    const size_t comparedIndex = llvm::find(columns, match._comparedColumn) - columns.begin();
    if (match._readsTheIDs && comparedIndex == columns.size()) {
        columns.push_back(match._comparedColumn);
        resultTypes.push_back(match._comparedColumn.getType());
    }

    builder.setInsertionPoint(match._filter);
    FilterOp fused = builder.create<FilterOp>(match._filter.getLoc(), resultTypes, mask, columns);

    for (size_t index = 0; index < keptColumns.size(); index++) {
        filtered[keptColumns[index]].replaceAllUsesWith(fused.getResult(index));
    }

    if (match._readsTheIDs) {
        builder.setInsertionPointAfter(fused);
        ElementID ids = builder.create<ElementID>(match._filter.getLoc(), match._unwoundColumn.getType(), fused.getResult(comparedIndex));

        for (const size_t index : elementsReadAsIDs) {
            filtered[index].replaceAllUsesWith(ids.getResult());
        }
    }

    match._filter.erase();
    match._equality.erase();
    match._product.erase();
}

struct FuseUnwindEquality : public impl::FuseUnwindEqualityBase<FuseUnwindEquality> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect matches first: fusing erases ops, which would invalidate the walk.
        llvm::SmallVector<UnwindEqualityCross> matches;
        root->walk([&matches](CrossProduct product) {
            UnwindEqualityCross match;
            if (matchUnwindEqualityCross(product, match)) {
                matches.push_back(match);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (UnwindEqualityCross& match : matches) {
            fuseUnwindEquality(match, builder);
        }
    }
};

bool isHop(Operation* op);
bool computesPerRow(Operation* op);
Value climbFilters(Value column);
void eraseIfUnused(Operation* op, mlir::RewriterBase& rewriter);
Value appendCarriedColumn(Operation* op, Value column, mlir::RewriterBase& rewriter);

bool computesFromTheRow(Operation* op) {
    if (op->getNumRegions() != 0) {
        return false;
    }

    const bool computesAValue = op->hasTrait<OpTrait::ConstantLike>()
                                || op->hasTrait<OpTrait::ConstantThroughOperands>();

    return computesAValue || computesPerRow(op) || isa<Range, MakeList, MakeMap, ListSlice>(op);
}

void collectRowCone(llvm::ArrayRef<Value> roots, Block* block, MaskCone& cone) {
    const auto computedInTheBlock = [block](Operation* op) {
        return op->getBlock() == block && computesFromTheRow(op);
    };

    llvm::SmallPtrSet<Operation*, 8> visited;
    for (const Value root : roots) {
        collectConePostOrder(root, computedInTheBlock, visited, cone);
    }
}

void cloneRowCone(const MaskCone& cone, mlir::IRMapping& mapping, mlir::RewriterBase& rewriter) {
    for (Operation* const op : cone._ops) {
        rewriter.clone(*op, mapping);
    }
}

void eraseUnusedRowCone(const MaskCone& cone, mlir::RewriterBase& rewriter) {
    for (Operation* const op : llvm::reverse(cone._ops)) {
        eraseIfUnused(op, rewriter);
    }
}

size_t leftFactorWidth(CrossProduct product) {
    return factorYieldColumns(product.getLeftFactor()).size();
}

void collectFactorTypes(Region& leftFactor, Region& rightFactor, llvm::SmallVectorImpl<Type>& types) {
    llvm::append_range(types, factorYieldColumns(leftFactor).getTypes());
    llvm::append_range(types, factorYieldColumns(rightFactor).getTypes());
}

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
    bool yieldsConstantColumn(Value column) {
        return ::db::yieldsConstantColumn(column, _constantColumns);
    }

    const ConjunctCone& getConjunctCone(Value conjunct, CrossProduct product);

    void clear() {
        _constantColumns.clear();
        _conjunctCones.clear();
    }

private:
    llvm::DenseMap<Value, bool> _constantColumns;
    llvm::DenseMap<std::pair<Value, Operation*>, std::unique_ptr<ConjunctCone>> _conjunctCones;
};

void collectFactorReads(const MaskCone& cone, CrossProduct product, ConeCache& cache, FactorReads& reads) {
    const size_t leftWidth = leftFactorWidth(product);
    for (const Value input : cone._inputs) {
        if (cache.yieldsConstantColumn(input)) {
            continue;
        }

        const Value column = climbFilters(input);
        if (column.getDefiningOp() != product.getOperation()) {
            reads._readsElsewhere = true;
            continue;
        }

        const size_t resultIndex = cast<OpResult>(column).getResultNumber();
        if (resultIndex < leftWidth) {
            reads._leftResults.push_back(resultIndex);
        } else {
            reads._rightResults.push_back(resultIndex);
        }
    }
}

const ConjunctCone& ConeCache::getConjunctCone(Value conjunct, CrossProduct product) {
    std::unique_ptr<ConjunctCone>& cached = _conjunctCones[{conjunct, product.getOperation()}];
    if (!cached) {
        cached = std::make_unique<ConjunctCone>();
        collectRowCone({conjunct}, product->getBlock(), cached->_cone);
        collectFactorReads(cached->_cone, product, *this, cached->_reads);
    }

    return *cached;
}

void appendCone(const MaskCone& from, llvm::SmallPtrSetImpl<Operation*>& appended, MaskCone& cone) {
    for (Operation* const op : from._ops) {
        if (appended.insert(op).second) {
            cone._ops.push_back(op);
        }
    }

    cone._inputs.insert(from._inputs.begin(), from._inputs.end());
}

void mapFactorReads(const MaskCone& cone, CrossProduct product, mlir::IRMapping& mapping) {
    for (const Value input : cone._inputs) {
        const Value column = climbFilters(input);
        if (column.getDefiningOp() == product.getOperation()) {
            const size_t resultIndex = cast<OpResult>(column).getResultNumber();
            mapping.map(input, productFactorColumn(product, resultIndex));
        }
    }
}

bool feedsOnlyTheChain(Operation* reader,
                       const llvm::SmallPtrSetImpl<Operation*>& chain,
                       llvm::SmallPtrSetImpl<Operation*>& visited) {
    Block* const block = reader->getBlock();

    llvm::SmallVector<Operation*> pending {reader};
    while (!pending.empty()) {
        Operation* const op = pending.pop_back_val();
        const bool seen = chain.contains(op) || !visited.insert(op).second;

        if (seen) {
            continue;
        } else if (op->getBlock() != block || !computesFromTheRow(op)) {
            return false;
        }

        llvm::append_range(pending, op->getUsers());
    }

    return true;
}

// Climbs from @param columns, the rows @param reader takes, through the filters stacked over a
// cross product down to it, and collects those filters top first. Moving work below the
// filters changes the rows of every level they stand on, so nothing but the filters, the
// masks they read and the reader may see those rows.
bool matchFilterChain(Operation* reader,
                      ValueRange columns,
                      CrossProduct& product,
                      llvm::SmallVectorImpl<FilterOp>& filters) {
    Block* const block = reader->getBlock();

    llvm::SmallPtrSet<Operation*, 8> chain {reader};
    llvm::SmallVector<Operation*, 4> levels;

    ValueRange levelColumns = columns;
    while (!product) {
        if (levelColumns.empty()) {
            return false;
        }

        Operation* const below = levelColumns.front().getDefiningOp();
        if (!below || below->getBlock() != block) {
            return false;
        }

        for (const Value column : levelColumns) {
            if (column.getDefiningOp() != below) {
                return false;
            }
        }

        levels.push_back(below);

        if (FilterOp filter = dyn_cast<FilterOp>(below)) {
            chain.insert(below);
            filters.push_back(filter);
            levelColumns = filter.getColumnsToFilter();
        } else if (CrossProduct belowProduct = dyn_cast<CrossProduct>(below)) {
            product = belowProduct;
        } else {
            return false;
        }
    }

    llvm::SmallPtrSet<Operation*, 16> visited;
    for (Operation* const level : levels) {
        for (Operation* const levelReader : level->getUsers()) {
            if (!feedsOnlyTheChain(levelReader, chain, visited)) {
                return false;
            }
        }
    }

    return true;
}

struct UnwindPlacement {
    Unwind _unwind {nullptr};
    MaskCone _source;
    CrossProduct _product {nullptr};
    llvm::SmallVector<FilterOp, 2> _filters;
    bool _sinksLeft {false};
    llvm::SmallVector<Operation*> _rowOps;
};

bool collectCarriedRowOps(Unwind unwind, ConeCache& cache, llvm::SmallVectorImpl<Operation*>& rowOps) {
    Block* const block = unwind->getBlock();

    llvm::SmallPtrSet<Operation*, 16> visited;
    const Operation::operand_range carried = unwind.getColumnsToFilter();
    llvm::SmallVector<Value> pending(carried.begin(), carried.end());
    while (!pending.empty()) {
        const Value value = pending.pop_back_val();
        if (cache.yieldsConstantColumn(value)) {
            continue;
        }

        Operation* const def = value.getDefiningOp();
        const bool madeInTheBlock = def && def->getBlock() == block && mlir::isMemoryEffectFree(def);

        if (!madeInTheBlock) {
            return false;
        } else if (!visited.insert(def).second) {
            continue;
        }

        rowOps.push_back(def);
        llvm::append_range(pending, def->getOperands());

        llvm::SetVector<Value> readFromAbove;
        mlir::getUsedValuesDefinedAbove(def->getRegions(), readFromAbove);
        llvm::append_range(pending, readFromAbove);
    }

    for (Operation* const rowOp : rowOps) {
        for (Operation* const user : rowOp->getUsers()) {
            Operation* const reader = block->findAncestorOpInBlock(*user);
            const bool readOutsideTheRows = reader != unwind.getOperation() && !visited.contains(reader);
            if (readOutsideTheRows) {
                return false;
            }
        }
    }

    llvm::sort(rowOps, [](Operation* lhs, Operation* rhs) {
        return lhs->isBeforeInBlock(rhs);
    });

    return true;
}

bool matchUnwindBesideItsRows(UnwindPlacement& placement, ConeCache& cache) {
    Unwind unwind = placement._unwind;

    for (const Value column : unwind.getColumnsToFilter()) {
        if (cache.yieldsConstantColumn(column) || !climbFilters(column).getDefiningOp<CrossProduct>()) {
            return false;
        }
    }

    return collectCarriedRowOps(unwind, cache, placement._rowOps);
}

bool matchUnwindIntoAFactor(UnwindPlacement& placement, ConeCache& cache) {
    Unwind unwind = placement._unwind;

    CrossProduct product;
    if (!matchFilterChain(unwind, unwind.getColumnsToFilter(), product, placement._filters)) {
        return false;
    }

    FactorReads sourceReads;
    collectFactorReads(placement._source, product, cache, sourceReads);

    const bool readsTheLeft = !sourceReads._leftResults.empty();
    const bool readsTheRight = !sourceReads._rightResults.empty();
    if (readsTheLeft == readsTheRight || sourceReads._readsElsewhere) {
        return false;
    }

    // Sunk below a filter reading the factor, the unwind would repeat the rows that filter
    // runs on once per element
    for (FilterOp filter : placement._filters) {
        const FactorReads& reads = cache.getConjunctCone(filter.getMask(), product)._reads;
        const bool readsTheFactor = readsTheLeft ? !reads._leftResults.empty() : !reads._rightResults.empty();
        if (readsTheFactor || reads._readsElsewhere) {
            return false;
        }
    }

    placement._product = product;
    placement._sinksLeft = readsTheLeft;

    return true;
}

bool matchUnwindPlacement(Unwind unwind, ConeCache& cache, UnwindPlacement& placement) {
    if (unwind.getColumnsToFilter().empty()) {
        return false;
    }

    placement._unwind = unwind;
    collectRowCone({unwind.getSource()}, unwind->getBlock(), placement._source);

    const bool readsARow = llvm::any_of(placement._source._inputs, [&cache](Value input) {
        return !cache.yieldsConstantColumn(input);
    });

    if (!readsARow) {
        return matchUnwindBesideItsRows(placement, cache);
    }

    return matchUnwindIntoAFactor(placement, cache);
}

void sinkUnwindIntoFactor(UnwindPlacement& placement, mlir::RewriterBase& rewriter) {
    Unwind unwind = placement._unwind;
    CrossProduct product = placement._product;

    Region& leftFactor = product.getLeftFactor();
    Region& rightFactor = product.getRightFactor();
    Region& factor = placement._sinksLeft ? leftFactor : rightFactor;

    const size_t leftWidth = leftFactorWidth(product);
    const size_t firstResult = placement._sinksLeft ? 0 : leftWidth;

    Yield yield = cast<Yield>(factor.front().getTerminator());
    const llvm::SmallVector<Value> yielded(yield.getColumns().begin(), yield.getColumns().end());

    mlir::IRMapping mapping;
    mapFactorReads(placement._source, product, mapping);

    rewriter.setInsertionPoint(yield);
    cloneRowCone(placement._source, mapping, rewriter);

    const Value unwoundElement = unwind.getElement();
    llvm::SmallVector<Type> sunkTypes {unwoundElement.getType()};
    for (const Value column : yielded) {
        sunkTypes.push_back(column.getType());
    }

    const Value source = mapping.lookupOrDefault(unwind.getSource());
    Unwind sunk = rewriter.create<Unwind>(unwind.getLoc(), sunkTypes, source, yielded);

    llvm::SmallVector<Value> widenedYield(sunk.getCarried().begin(), sunk.getCarried().end());
    widenedYield.push_back(sunk.getElement());

    rewriter.modifyOpInPlace(yield, [&yield, &widenedYield]() {
        yield->setOperands(widenedYield);
    });

    llvm::SmallVector<Type> productTypes;
    collectFactorTypes(leftFactor, rightFactor, productTypes);

    rewriter.setInsertionPoint(product);
    CrossProduct widened = rewriter.create<CrossProduct>(product.getLoc(), productTypes);
    widened.getLeftFactor().takeBody(leftFactor);
    widened.getRightFactor().takeBody(rightFactor);

    // The element follows the factor's columns, so a right factor sitting after a widened
    // left one starts a column further along
    for (const Value result : product.getResults()) {
        const size_t resultIndex = cast<OpResult>(result).getResultNumber();
        const bool shifts = placement._sinksLeft && resultIndex >= leftWidth;

        rewriter.replaceAllUsesWith(result, widened.getResult(shifts ? resultIndex + 1 : resultIndex));
    }

    Value element = widened.getResult(firstResult + yielded.size());
    for (FilterOp filter : llvm::reverse(placement._filters)) {
        element = appendCarriedColumn(filter, element, rewriter);
    }

    rewriter.replaceAllUsesWith(unwoundElement, element);

    const Operation::operand_range carried = unwind.getColumnsToFilter();
    const ResultRange carriedRows = unwind.getCarried();
    for (size_t index = 0; index < carried.size(); index++) {
        rewriter.replaceAllUsesWith(carriedRows[index], carried[index]);
    }

    rewriter.eraseOp(unwind);
    eraseUnusedRowCone(placement._source, rewriter);
    rewriter.eraseOp(product);
}

void unwindBesideItsRows(UnwindPlacement& placement, mlir::RewriterBase& rewriter) {
    Unwind unwind = placement._unwind;
    const Location loc = unwind.getLoc();

    const Operation::operand_range carried = unwind.getColumnsToFilter();
    const Value unwoundElement = unwind.getElement();
    const Type elementType = unwoundElement.getType();

    const Operation::operand_range::type_range carriedTypes = carried.getTypes();
    llvm::SmallVector<Type> productTypes(carriedTypes.begin(), carriedTypes.end());
    productTypes.push_back(elementType);

    rewriter.setInsertionPoint(unwind);
    CrossProduct product = rewriter.create<CrossProduct>(loc, productTypes);

    Block* const rowsBlock = &product.getLeftFactor().front();
    rewriter.setInsertionPointToEnd(rowsBlock);
    Yield rowsYield = rewriter.create<Yield>(loc, carried);

    for (Operation* const rowOp : placement._rowOps) {
        rewriter.moveOpBefore(rowOp, rowsYield);
    }

    Block* const listBlock = &product.getRightFactor().front();
    rewriter.setInsertionPointToEnd(listBlock);

    mlir::IRMapping mapping;
    cloneRowCone(placement._source, mapping, rewriter);

    const Value source = mapping.lookupOrDefault(unwind.getSource());
    Unwind listUnwind = rewriter.create<Unwind>(loc, TypeRange {elementType}, source, ValueRange {});
    rewriter.create<Yield>(loc, ValueRange {listUnwind.getElement()});

    const ResultRange carriedRows = unwind.getCarried();
    for (size_t index = 0; index < carriedRows.size(); index++) {
        rewriter.replaceAllUsesWith(carriedRows[index], product.getResult(index));
    }

    rewriter.replaceAllUsesWith(unwoundElement, product.getResult(carriedRows.size()));

    rewriter.eraseOp(unwind);
    eraseUnusedRowCone(placement._source, rewriter);
}

void placeUnwind(UnwindPlacement& placement, mlir::RewriterBase& rewriter) {
    if (placement._product) {
        sinkUnwindIntoFactor(placement, rewriter);
    } else {
        unwindBesideItsRows(placement, rewriter);
    }
}

CrossProduct matchProductFactor(Region& factor) {
    Block& block = factor.front();
    if (block.getOperations().size() != 2) {
        return nullptr;
    }

    CrossProduct nested = dyn_cast<CrossProduct>(block.front());
    if (!nested) {
        return nullptr;
    }

    for (const Value column : factorYieldColumns(factor)) {
        if (column.getDefiningOp() != nested.getOperation()) {
            return nullptr;
        }
    }

    return nested;
}

// A product nested as one factor of another, regrouped so a conjunct reading the other
// factor and one side of the nested product reads one factor: the nested side it reads and
// the other factor are crossed first, and the nested side it does not read is crossed after
struct ProductRotation {
    bool _rotates {false};
    bool _nestedOnTheLeft {false};
    bool _keepsTheNestedLeft {false};
};

struct ProductCut {
    FilterOp _filter {nullptr};
    CrossProduct _product {nullptr};
    llvm::SmallVector<Value, 2> _leftConjuncts;
    llvm::SmallVector<Value, 2> _rightConjuncts;
    llvm::SmallVector<Value, 2> _keptConjuncts;
    MaskCone _leftCone;
    MaskCone _rightCone;
    ProductRotation _rotation;
};

bool readOneNestedSide(CrossProduct product,
                       bool nestedOnTheLeft,
                       llvm::ArrayRef<size_t> results,
                       bool& readsTheNestedLeft) {
    Region& factor = nestedOnTheLeft ? product.getLeftFactor() : product.getRightFactor();
    const CrossProduct nested = matchProductFactor(factor);
    if (!nested || results.empty()) {
        return false;
    }

    const size_t nestedLeftWidth = leftFactorWidth(nested);

    bool readsTheLeft = false;
    bool readsTheRight = false;
    for (const size_t resultIndex : results) {
        const Value column = productFactorColumn(product, resultIndex);
        const size_t nestedIndex = cast<OpResult>(column).getResultNumber();
        if (nestedIndex < nestedLeftWidth) {
            readsTheLeft = true;
        } else {
            readsTheRight = true;
        }
    }

    readsTheNestedLeft = readsTheLeft;

    return readsTheLeft != readsTheRight;
}

bool matchRotation(const FactorReads& reads, CrossProduct product, ProductRotation& rotation) {
    const bool relatesBothFactors = !reads._leftResults.empty() && !reads._rightResults.empty();
    if (!relatesBothFactors || reads._readsElsewhere) {
        return false;
    }

    bool readsTheNestedLeft = false;
    if (readOneNestedSide(product, true, reads._leftResults, readsTheNestedLeft)) {
        rotation = ProductRotation {true, true, readsTheNestedLeft};
        return true;
    } else if (readOneNestedSide(product, false, reads._rightResults, readsTheNestedLeft)) {
        rotation = ProductRotation {true, false, readsTheNestedLeft};
        return true;
    }

    return false;
}

bool matchProductCut(FilterOp filter, ConeCache& cache, ProductCut& cut) {
    llvm::SmallVector<FilterOp, 2> filters;
    if (!matchFilterChain(filter, filter.getColumnsToFilter(), cut._product, filters)) {
        return false;
    }

    cut._filter = filter;

    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    llvm::SmallPtrSet<Operation*, 16> appendedLeft;
    llvm::SmallPtrSet<Operation*, 16> appendedRight;
    for (const Value conjunct : conjuncts) {
        const ConjunctCone& conjunctCone = cache.getConjunctCone(conjunct, cut._product);
        const FactorReads& reads = conjunctCone._reads;

        const bool readsTheLeft = !reads._leftResults.empty();
        const bool readsTheRight = !reads._rightResults.empty();
        const bool readsOneFactor = readsTheLeft != readsTheRight && !reads._readsElsewhere;

        if (readsOneFactor && readsTheLeft) {
            cut._leftConjuncts.push_back(conjunct);
            appendCone(conjunctCone._cone, appendedLeft, cut._leftCone);
        } else if (readsOneFactor) {
            cut._rightConjuncts.push_back(conjunct);
            appendCone(conjunctCone._cone, appendedRight, cut._rightCone);
        } else {
            cut._keptConjuncts.push_back(conjunct);

            if (!cut._rotation._rotates) {
                matchRotation(reads, cut._product, cut._rotation);
            }
        }
    }

    const bool pushes = !cut._leftConjuncts.empty() || !cut._rightConjuncts.empty();
    if (pushes) {
        cut._rotation._rotates = false;
    }

    return pushes || cut._rotation._rotates;
}

Value conjoin(llvm::ArrayRef<Value> conjuncts, Location loc, mlir::RewriterBase& rewriter) {
    Value mask = conjuncts.front();
    for (const Value conjunct : conjuncts.drop_front()) {
        mask = rewriter.create<AndOp>(loc, mask.getType(), mask, conjunct).getResult();
    }

    return mask;
}

void cutFactor(CrossProduct product,
               bool leftFactor,
               llvm::ArrayRef<Value> conjuncts,
               const MaskCone& cone,
               Location loc,
               mlir::RewriterBase& rewriter) {
    if (conjuncts.empty()) {
        return;
    }

    Region& factor = leftFactor ? product.getLeftFactor() : product.getRightFactor();
    Yield yield = cast<Yield>(factor.front().getTerminator());
    const Operation::operand_range yieldColumns = yield.getColumns();
    const llvm::SmallVector<Value> yielded(yieldColumns.begin(), yieldColumns.end());
    const Operation::operand_range::type_range yieldTypes = yieldColumns.getTypes();
    const llvm::SmallVector<Type> types(yieldTypes.begin(), yieldTypes.end());

    mlir::IRMapping mapping;
    mapFactorReads(cone, product, mapping);

    rewriter.setInsertionPoint(yield);
    cloneRowCone(cone, mapping, rewriter);

    llvm::SmallVector<Value, 2> mappedConjuncts;
    for (const Value conjunct : conjuncts) {
        mappedConjuncts.push_back(mapping.lookup(conjunct));
    }

    const Value mask = conjoin(mappedConjuncts, loc, rewriter);
    FilterOp cut = rewriter.create<FilterOp>(loc, types, mask, yielded);

    rewriter.modifyOpInPlace(yield, [&yield, &cut]() {
        yield->setOperands(cut.getResults());
    });
}

void eraseUnusedConjunction(Value mask, mlir::RewriterBase& rewriter) {
    llvm::SmallVector<Value> pending {mask};
    while (!pending.empty()) {
        AndOp conjunction = pending.pop_back_val().getDefiningOp<AndOp>();
        const bool unusedConjunction = conjunction && conjunction.getResult().use_empty();
        if (!unusedConjunction) {
            continue;
        }

        pending.push_back(conjunction.getLhs());
        pending.push_back(conjunction.getRhs());
        rewriter.eraseOp(conjunction);
    }
}

void pushCutIntoFactors(ProductCut& cut, mlir::RewriterBase& rewriter) {
    FilterOp filter = cut._filter;
    const Location loc = filter.getLoc();
    const Value mask = filter.getMask();

    cutFactor(cut._product, true, cut._leftConjuncts, cut._leftCone, loc, rewriter);
    cutFactor(cut._product, false, cut._rightConjuncts, cut._rightCone, loc, rewriter);

    if (cut._keptConjuncts.empty()) {
        bypassFilter(filter);
        rewriter.eraseOp(filter);
    } else {
        rewriter.setInsertionPoint(filter);
        const Value keptMask = conjoin(cut._keptConjuncts, loc, rewriter);

        rewriter.modifyOpInPlace(filter, [&filter, keptMask]() {
            filter.getMaskMutable().assign(keptMask);
        });
    }

    eraseUnusedConjunction(mask, rewriter);
    eraseUnusedRowCone(cut._leftCone, rewriter);
    eraseUnusedRowCone(cut._rightCone, rewriter);
}

void rotateProduct(CrossProduct product, const ProductRotation& rotation, mlir::RewriterBase& rewriter) {
    const Location loc = product.getLoc();

    Region& leftFactor = product.getLeftFactor();
    Region& rightFactor = product.getRightFactor();
    Region& nestedFactor = rotation._nestedOnTheLeft ? leftFactor : rightFactor;
    Region& otherFactor = rotation._nestedOnTheLeft ? rightFactor : leftFactor;

    CrossProduct nested = matchProductFactor(nestedFactor);
    Region& keptFactor = rotation._keepsTheNestedLeft ? nested.getLeftFactor() : nested.getRightFactor();
    Region& restFactor = rotation._keepsTheNestedLeft ? nested.getRightFactor() : nested.getLeftFactor();

    const Operation::operand_range nestedYield = factorYieldColumns(nestedFactor);

    const size_t productLeftWidth = leftFactorWidth(product);
    const size_t nestedLeftWidth = leftFactorWidth(nested);
    const size_t keptWidth = factorYieldColumns(keptFactor).size();
    const size_t otherWidth = factorYieldColumns(otherFactor).size();

    // The other factor keeps its side, so an outer factor is not re-run once per chunk of the
    // nested side it is crossed with
    Region& innerLeft = rotation._nestedOnTheLeft ? keptFactor : otherFactor;
    Region& innerRight = rotation._nestedOnTheLeft ? otherFactor : keptFactor;
    const size_t keptFirst = rotation._nestedOnTheLeft ? 0 : otherWidth;
    const size_t otherFirstInInner = rotation._nestedOnTheLeft ? keptWidth : 0;

    llvm::SmallVector<Type> innerTypes;
    collectFactorTypes(innerLeft, innerRight, innerTypes);

    llvm::SmallVector<Type> rotatedTypes(innerTypes);
    llvm::append_range(rotatedTypes, factorYieldColumns(restFactor).getTypes());

    rewriter.setInsertionPoint(product);
    CrossProduct rotated = rewriter.create<CrossProduct>(loc, rotatedTypes);
    rotated.getRightFactor().takeBody(restFactor);

    rewriter.setInsertionPointToEnd(&rotated.getLeftFactor().front());
    CrossProduct inner = rewriter.create<CrossProduct>(loc, innerTypes);
    rewriter.create<Yield>(loc, inner.getResults());

    inner.getLeftFactor().takeBody(innerLeft);
    inner.getRightFactor().takeBody(innerRight);

    // The rotated product holds the inner product's columns, then the rest of the nested
    // product's
    const auto rotatedIndex = [&](size_t resultIndex) {
        const bool fromTheNested = (resultIndex < productLeftWidth) == rotation._nestedOnTheLeft;
        if (!fromTheNested) {
            const size_t otherFirst = rotation._nestedOnTheLeft ? productLeftWidth : 0;
            return otherFirstInInner + (resultIndex - otherFirst);
        }

        const size_t nestedFirst = rotation._nestedOnTheLeft ? 0 : productLeftWidth;
        const Value column = nestedYield[resultIndex - nestedFirst];
        const size_t nestedIndex = cast<OpResult>(column).getResultNumber();

        const bool fromTheLeft = nestedIndex < nestedLeftWidth;
        const size_t sidePosition = fromTheLeft ? nestedIndex : nestedIndex - nestedLeftWidth;
        if (fromTheLeft == rotation._keepsTheNestedLeft) {
            return keptFirst + sidePosition;
        }

        return keptWidth + otherWidth + sidePosition;
    };

    for (const Value result : product.getResults()) {
        const size_t resultIndex = cast<OpResult>(result).getResultNumber();
        rewriter.replaceAllUsesWith(result, rotated.getResult(rotatedIndex(resultIndex)));
    }

    rewriter.eraseOp(product);
}

// A rotation is pushed through in the same rewrite: the filter then stands in the factor the
// rotation made, which is no bare product any more, so no other filter can rotate it back
void regroupProductCut(ProductCut& cut, ConeCache& cache, mlir::RewriterBase& rewriter) {
    if (!cut._rotation._rotates) {
        pushCutIntoFactors(cut, rewriter);
        return;
    }

    rotateProduct(cut._product, cut._rotation, rewriter);
    cache.clear();

    ProductCut rotatedCut;
    const bool pushes = matchProductCut(cut._filter, cache, rotatedCut) && !rotatedCut._rotation._rotates;
    bioassert(pushes, "A rotated product leaves the conjunct that rotated it on one factor");

    pushCutIntoFactors(rotatedCut, rewriter);
}

bool placeInAFactor(Operation* op, ConeCache& cache, mlir::RewriterBase& rewriter) {
    FilterOp filter = dyn_cast<FilterOp>(op);

    ProductCut cut;
    UnwindPlacement placement;
    if (filter && matchProductCut(filter, cache, cut)) {
        regroupProductCut(cut, cache, rewriter);
    } else if (!filter && matchUnwindPlacement(cast<Unwind>(op), cache, placement)) {
        placeUnwind(placement, rewriter);
    } else {
        return false;
    }

    cache.clear();
    return true;
}

// A filter moved into a factor can let an unwind sink below the filters left over the
// product, and a sunk unwind can leave a filter over a product it then moves into: one
// worklist over both drives the two to a fixed point
struct PlaceInFactors : public impl::PlaceInFactorsBase<PlaceInFactors> {
    void runOnOperation() override {
        ConeCache cache;
        runWorklist<FilterOp, Unwind>(getOperation(), [&cache](Operation* op, mlir::RewriterBase& rewriter) {
            return placeInAFactor(op, cache, rewriter);
        });
    }
};

struct NodeIDScanChain {
    ScanSource _source;
    llvm::SmallVector<int64_t> _nodeIDs;
    bool _afterThePattern {false};
};

Value disjunctionColumn(Value mask) {
    while (OrOp disjunction = mask.getDefiningOp<OrOp>()) {
        mask = disjunction.getLhs();
    }

    EqOp equality = mask.getDefiningOp<EqOp>();
    if (!equality) {
        return {};
    }

    const Value lhs = equality.getLhs();
    return lhs.getDefiningOp<ConstantOp>() ? equality.getRhs() : lhs;
}

bool keepsTheScannedNode(Operation& op) {
    ExplorePaths exploration = dyn_cast<ExplorePaths>(op);
    const bool walksEachSeed = exploration && !exploration.getDistinct();

    return isa<FilterOp>(op) || walksEachSeed || isHop(&op) || computesPerRow(&op);
}

// Whether the rows the source starts reach the filter only through ops keepsTheRows accepts,
// with nothing else reading them before it
bool rowsReachTheFilter(Operation* source, FilterOp filter, bool (*keepsTheRows)(Operation&)) {
    llvm::SmallPtrSet<Operation*, 16> readers {source};

    for (Operation& op : llvm::make_range(std::next(Block::iterator(source)), Block::iterator(filter.getOperation()))) {
        const bool readsTheRows = llvm::any_of(op.getOperands(), [&readers](Value operand) {
            Operation* const def = operand.getDefiningOp();
            return def && readers.contains(def);
        });

        if (!readsTheRows) {
            continue;
        }

        if (!keepsTheRows(op)) {
            return false;
        }

        readers.insert(&op);
    }

    for (Operation* const reader : readers) {
        for (Operation* const user : reader->getUsers()) {
            if (user != filter.getOperation() && !readers.contains(user)) {
                return false;
            }
        }
    }

    return true;
}

// A disjunction over the scanned node read after the hops and filters of the pattern
// rooted at the scan: it restricts the scan itself
bool matchPatternScanSource(FilterOp filter, Value column, LineageClimbs& climbs, ScanSource& source) {
    const Value anchor = climbs.anchorOf(column);
    Operation* const scan = anchor ? anchor.getDefiningOp() : nullptr;
    const bool isNodeScan = isa_and_nonnull<ScanNodes, ScanNodesByLabel>(scan);
    if (!isNodeScan || scan->getBlock() != filter->getBlock()) {
        return false;
    }

    if (!rowsReachTheFilter(scan, filter, keepsTheScannedNode)) {
        return false;
    }

    source._op = scan;
    source._column = column;
    source._labels = ArrayAttr();

    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(scan)) {
        source._labels = scanByLabel.getLabels();
    }

    return true;
}

bool matchNodeIDScanChain(FilterOp filter, LineageClimbs& climbs, NodeIDScanChain& chain) {
    const Value mask = filter.getMask();

    if (matchSoleScanSource(filter, chain._source)) {
        if (!collectNodeIDDisjunction(mask, chain._source._column, chain._nodeIDs)) {
            return false;
        }
    } else {
        const Value column = disjunctionColumn(mask);
        if (!column || !collectNodeIDDisjunction(mask, column, chain._nodeIDs)) {
            return false;
        }

        if (!matchPatternScanSource(filter, column, climbs, chain._source)) {
            return false;
        }

        chain._afterThePattern = true;
    }

    // A scan yields each node once in ID order, so the filter did too.
    llvm::sort(chain._nodeIDs);
    chain._nodeIDs.erase(std::unique(chain._nodeIDs.begin(), chain._nodeIDs.end()), chain._nodeIDs.end());

    return true;
}

void fuseScanByNodeIDs(FilterOp filter, const NodeIDScanChain& chain, mlir::OpBuilder& builder) {
    const mlir::Location loc = filter.getLoc();
    const Type nodeColumnType = chain._source._column.getType();

    builder.setInsertionPoint(chain._afterThePattern ? chain._source._op : filter.getOperation());
    ConstScanNodes constScan = builder.create<ConstScanNodes>(loc, nodeColumnType, chain._nodeIDs);

    llvm::SmallVector<Value> fused {constScan.getResult()};
    if (chain._source._labels) {
        keepLabelledRows(fused, chain._source._labels, loc, builder);
    }

    if (!chain._afterThePattern) {
        replaceFilterWithSource(filter, fused.front(), chain._source._op, collectMaskCone(filter.getMask()));
        return;
    }

    // The listed nodes start the pattern in the scan's place, and every row it builds
    // from them holds the disjunction
    Operation* const scan = chain._source._op;
    scan->getResult(0).replaceAllUsesWith(fused.front());
    scan->erase();

    const MaskCone cone = collectMaskCone(filter.getMask());
    bypassFilter(filter);
    filter.erase();

    for (Operation* const coneOp : llvm::reverse(cone._ops)) {
        eraseIfUnused(coneOp);
    }
}

struct FuseScanByNodeIDs : public impl::FuseScanByNodeIDsBase<FuseScanByNodeIDs> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        LineageClimbs climbs;

        const auto matchChain = [&climbs](FilterOp filter, NodeIDScanChain& chain) {
            return matchNodeIDScanChain(filter, climbs, chain);
        };

        const auto fuseChain = [&climbs](FilterOp filter, const NodeIDScanChain& chain, mlir::OpBuilder& builder) {
            climbs.clear();
            fuseScanByNodeIDs(filter, chain, builder);
        };

        runFilterPass<NodeIDScanChain>(getOperation(), matchChain, fuseChain, builder);
    }
};

// A hop over a whole-graph node scan walks every node's edges, which is the whole edge
// set - what a scan_edges produces on its own.
bool matchWholeEdgeSetHop(Operation* hop, ScanNodes& scan) {
    // get_edges walks both directions, so over every node it reports each edge twice, once
    // from each endpoint, and the by-type hops keep one type. Only the directed untyped
    // hops read the edge set a scan_edges produces.
    if (!isa<GetOutEdges, GetInEdges>(hop)) {
        return false;
    }

    // Operand 0 is the input node column and anything after it is a carry set, row-aligned
    // with the scan and with no counterpart in a scan_edges to be rewired to.
    if (hop->getNumOperands() != 1) {
        return false;
    }

    scan = hop->getOperand(0).getDefiningOp<ScanNodes>();
    if (!scan) {
        return false;
    }

    // The fused form drops the node column, so a second reader of it keeps the scan alive.
    return scan.getResult().hasOneUse();
}

void fuseScanEdges(Operation* hop, ScanNodes scan, mlir::OpBuilder& builder) {
    builder.setInsertionPoint(hop);

    // Both directed hops fill srcids and tgtids with the edge's own source and target -
    // the direction only decided which side was walked from - so the hop's four results
    // map one-for-one onto the edge scan's, which are declared in the same order.
    ScanEdges edgeScan = builder.create<ScanEdges>(hop->getLoc(),
                                                   hop->getResult(0).getType(),
                                                   hop->getResult(1).getType(),
                                                   hop->getResult(2).getType(),
                                                   hop->getResult(3).getType());

    hop->replaceAllUsesWith(edgeScan.getOperation());
    hop->erase();
    scan.erase();
}

struct FuseScanEdges : public impl::FuseScanEdgesBase<FuseScanEdges> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect first: fusing erases the hop and its scan, which would invalidate the walk.
        llvm::SmallVector<Operation*> hops;
        root->walk([&](Operation* op) {
            ScanNodes scan;
            if (matchWholeEdgeSetHop(op, scan)) {
                hops.push_back(op);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (Operation* const hop : hops) {
            ScanNodes scan;
            if (!matchWholeEdgeSetHop(hop, scan)) {
                continue;
            }

            fuseScanEdges(hop, scan, builder);
        }
    }
};

// A directed hop whose rows are then cut down to the types the pattern named: the by-type
// hop spelled the long way, since the walk itself can keep the edges of those types and
// never build the rows the filter goes on to drop.
struct TypedHop {
    Operation* _hop {nullptr};
    CheckEdgeTypeConstraint _check;
    ArrayAttr _edgeTypes;
};

bool matchTypedHop(FilterOp filter, TypedHop& typedHop) {
    CheckEdgeTypeConstraint check = filter.getMask().getDefiningOp<CheckEdgeTypeConstraint>();
    if (!check) {
        return false;
    }

    // The hop walks the disjunction itself, so any number of required types fuses.
    const ArrayAttr edgeTypes = check.getEdgeTypes();
    if (edgeTypes.empty()) {
        return false;
    }

    const Value edgeTypeIds = check.getEdgeTypeIds();
    Operation* const hop = edgeTypeIds.getDefiningOp();
    if (!hop || !isa<GetOutEdges, GetInEdges>(hop)) {
        return false;
    }

    constexpr size_t etypesResultIndex = 2;
    if (edgeTypeIds != hop->getResult(etypesResultIndex)) {
        return false;
    }

    // Every column the filter cuts has to be one the hop bound, or the fused hop has nothing
    // of its own to hand back in its place.
    for (const Value column : filter.getColumnsToFilter()) {
        if (column.getDefiningOp() != hop) {
            return false;
        }
    }

    // And nothing outside the pair may read the hop, or that reader would go on seeing the
    // rows the type turns away.
    for (const Value result : hop->getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsThePair = user == filter.getOperation() || user == check.getOperation();
            if (!readsThePair) {
                return false;
            }
        }
    }

    typedHop = TypedHop {._hop = hop, ._check = check, ._edgeTypes = edgeTypes};

    return true;
}

template <typename ByTypeOp>
Operation* createByTypeHop(Operation* hop, ArrayAttr edgeTypes, mlir::OpBuilder& builder) {
    const Operation::result_range results = hop->getResults();

    ByTypeOp byTypeHop = builder.create<ByTypeOp>(hop->getLoc(),
                                                  results[0].getType(),
                                                  results[1].getType(),
                                                  results[2].getType(),
                                                  results[3].getType(),
                                                  results.drop_front(hopFixedResultCount).getTypes(),
                                                  hop->getOperand(0),
                                                  edgeTypes,
                                                  hop->getOperands().drop_front());

    return byTypeHop.getOperation();
}

void fuseEdgesByType(FilterOp filter, const TypedHop& typedHop, mlir::OpBuilder& builder) {
    Operation* const hop = typedHop._hop;

    builder.setInsertionPoint(hop);

    // A by-type hop declares the same four fixed results and the same carry set behind them,
    // so the plain hop's results map onto it one for one.
    Operation* const byTypeHop = isa<GetOutEdges>(hop)
                                     ? createByTypeHop<GetOutEdgesByType>(hop, typedHop._edgeTypes, builder)
                                     : createByTypeHop<GetInEdgesByType>(hop, typedHop._edgeTypes, builder);

    hop->replaceAllUsesWith(byTypeHop);

    // The hop now yields the rows the filter used to leave, so each column the filter handed
    // on is the one it was given.
    const Operation::operand_range columns = filter.getColumnsToFilter();
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    filter.erase();
    eraseIfUnused(typedHop._check);
    hop->erase();
}

struct FuseEdgesByType : public impl::FuseEdgesByTypeBase<FuseEdgesByType> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect first: fusing erases the filter, its check and its hop, which would
        // invalidate the walk.
        llvm::SmallVector<FilterOp> filters;
        root->walk([&](FilterOp filter) {
            TypedHop typedHop;
            if (matchTypedHop(filter, typedHop)) {
                filters.push_back(filter);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (FilterOp filter : filters) {
            TypedHop typedHop;
            if (!matchTypedHop(filter, typedHop)) {
                continue;
            }

            fuseEdgesByType(filter, typedHop, builder);
        }
    }
};

// An edge scan whose rows are then cut down to the types the pattern named: the by-type
// scan spelled the long way, since the scan itself can keep the edges of those types and
// never build the rows the filter goes on to drop.
struct TypedEdgeScan {
    ScanEdges _scan;
    CheckEdgeTypeConstraint _check;
    ArrayAttr _edgeTypes;
};

bool matchTypedEdgeScan(FilterOp filter, TypedEdgeScan& typedScan) {
    CheckEdgeTypeConstraint check = filter.getMask().getDefiningOp<CheckEdgeTypeConstraint>();
    if (!check) {
        return false;
    }

    // As in matchTypedHop: the scan walks the disjunction itself, so any number of
    // required types fuses.
    const ArrayAttr edgeTypes = check.getEdgeTypes();
    if (edgeTypes.empty()) {
        return false;
    }

    const Value edgeTypeIds = check.getEdgeTypeIds();
    ScanEdges scan = edgeTypeIds.getDefiningOp<ScanEdges>();
    if (!scan) {
        return false;
    }

    if (edgeTypeIds != scan.getEtypes()) {
        return false;
    }

    for (const Value column : filter.getColumnsToFilter()) {
        if (column.getDefiningOp() != scan.getOperation()) {
            return false;
        }
    }

    Operation* const filterOp = filter.getOperation();
    Operation* const checkOp = check.getOperation();
    for (const Value result : scan->getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsThePair = user == filterOp || user == checkOp;
            if (!readsThePair) {
                return false;
            }
        }
    }

    typedScan = TypedEdgeScan {._scan = scan, ._check = check, ._edgeTypes = edgeTypes};

    return true;
}

void fuseScanEdgesByType(FilterOp filter, const TypedEdgeScan& typedScan, mlir::OpBuilder& builder) {
    ScanEdges scan = typedScan._scan;
    Operation* const scanOp = scan.getOperation();

    builder.setInsertionPoint(scanOp);

    // A by-type scan declares the same four results in the same order, so the plain scan's
    // map onto it one for one.
    ScanEdgesByType byTypeScan = builder.create<ScanEdgesByType>(scan.getLoc(),
                                                                 scan.getSrcids().getType(),
                                                                 scan.getEids().getType(),
                                                                 scan.getEtypes().getType(),
                                                                 scan.getTgtids().getType(),
                                                                 typedScan._edgeTypes);

    scanOp->replaceAllUsesWith(byTypeScan.getOperation());

    const Operation::operand_range columns = filter.getColumnsToFilter();
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    filter.erase();
    eraseIfUnused(typedScan._check);
    scanOp->erase();
}

struct FuseScanEdgesByType : public impl::FuseScanEdgesByTypeBase<FuseScanEdgesByType> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect first: fusing erases the filter, its check and its scan, which would
        // invalidate the walk.
        llvm::SmallVector<FilterOp> filters;
        root->walk([&](FilterOp filter) {
            TypedEdgeScan typedScan;
            if (matchTypedEdgeScan(filter, typedScan)) {
                filters.push_back(filter);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (FilterOp filter : filters) {
            TypedEdgeScan typedScan;
            if (!matchTypedEdgeScan(filter, typedScan)) {
                continue;
            }

            fuseScanEdgesByType(filter, typedScan, builder);
        }
    }
};

// A WHERE spelling a type disjunction reaches here as one check per type OR-ed together,
// which no by-type fusion can read. Two checks over the same type column are one check over
// the combined set - the union for an OR, the intersection for an AND - and that single
// check is what the by-type reads below can take.
struct CombinedTypeChecks {
    CheckEdgeTypeConstraint _left;
    CheckEdgeTypeConstraint _right;
};

bool matchCombinedTypeChecks(Operation* op, CombinedTypeChecks& combined) {
    if (!isa<AndOp, OrOp>(op)) {
        return false;
    }

    CheckEdgeTypeConstraint left = op->getOperand(0).getDefiningOp<CheckEdgeTypeConstraint>();
    CheckEdgeTypeConstraint right = op->getOperand(1).getDefiningOp<CheckEdgeTypeConstraint>();
    if (!left || !right) {
        return false;
    }

    // Both checks must read the same column, or they are asking about different edges
    if (left.getEdgeTypeIds() != right.getEdgeTypeIds()) {
        return false;
    }

    if (left.getNullable() != right.getNullable()) {
        return false;
    }

    combined = CombinedTypeChecks {._left = left, ._right = right};

    return true;
}

void combineEdgeTypes(bool isUnion,
                      ArrayAttr left,
                      ArrayAttr right,
                      llvm::SmallVectorImpl<Attribute>& edgeTypes) {
    if (isUnion) {
        edgeTypes.assign(left.begin(), left.end());

        for (const Attribute edgeType : right) {
            if (!llvm::is_contained(edgeTypes, edgeType)) {
                edgeTypes.push_back(edgeType);
            }
        }

        return;
    }

    for (const Attribute edgeType : left) {
        if (llvm::is_contained(right, edgeType)) {
            edgeTypes.push_back(edgeType);
        }
    }
}

void fuseTypeChecks(Operation* op, const CombinedTypeChecks& combined, mlir::OpBuilder& builder) {
    const bool isUnion = isa<OrOp>(op);

    // The op wrappers are handles, so copying them out of the const match is what lets their
    // accessors be called
    CheckEdgeTypeConstraint left = combined._left;
    CheckEdgeTypeConstraint right = combined._right;

    llvm::SmallVector<Attribute, 4> edgeTypes;
    combineEdgeTypes(isUnion, left.getEdgeTypes(), right.getEdgeTypes(), edgeTypes);

    // An edge carries one type, so an AND over two disjoint sets is a predicate no edge
    // passes. The check op has no way to spell that, so the pair stays as it is.
    if (edgeTypes.empty()) {
        return;
    }

    builder.setInsertionPoint(op);

    CheckEdgeTypeConstraint fused = builder.create<CheckEdgeTypeConstraint>(op->getLoc(),
                                                                            op->getResult(0).getType(),
                                                                            left.getEdgeTypeIds(),
                                                                            builder.getArrayAttr(edgeTypes),
                                                                            left.getNullable());

    op->getResult(0).replaceAllUsesWith(fused.getResult());
    op->erase();

    Operation* const leftOp = left.getOperation();
    Operation* const rightOp = right.getOperation();

    eraseIfUnused(leftOp);
    if (rightOp != leftOp) {
        eraseIfUnused(rightOp);
    }
}

struct FuseEdgeTypePredicates : public impl::FuseEdgeTypePredicatesBase<FuseEdgeTypePredicates> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect first, as every rewrite here erases ops the walk would still visit. Program
        // order then folds a chain of ORs in one run: an operand is already one check by the
        // time the op combining it is reached.
        llvm::SmallVector<Operation*> predicates;
        root->walk([&](Operation* op) {
            if (isa<AndOp, OrOp>(op)) {
                predicates.push_back(op);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (Operation* const op : predicates) {
            CombinedTypeChecks combined;
            if (!matchCombinedTypeChecks(op, combined)) {
                continue;
            }

            fuseTypeChecks(op, combined, builder);
        }
    }
};

bool containsLabels(ArrayAttr alternative, ArrayAttr labels) {
    return llvm::all_of(labels, [alternative](Attribute label) {
        return llvm::is_contained(alternative, label);
    });
}

// An alternative asking for every label of another one keeps no node that one does not, so
// it is dropped, and of two asking for the same labels in a different order only the first stays
void keepWeakestAlternatives(llvm::ArrayRef<Attribute> alternatives, llvm::SmallVectorImpl<Attribute>& kept) {
    for (size_t index = 0; index < alternatives.size(); index++) {
        const ArrayAttr alternative = cast<ArrayAttr>(alternatives[index]);

        bool subsumed = false;
        for (size_t otherIndex = 0; otherIndex < alternatives.size() && !subsumed; otherIndex++) {
            const ArrayAttr other = cast<ArrayAttr>(alternatives[otherIndex]);
            const bool otherIsWeaker = otherIndex != index && containsLabels(alternative, other);
            const bool sameLabels = containsLabels(other, alternative);
            subsumed = otherIsWeaker && (!sameLabels || otherIndex < index);
        }

        if (!subsumed) {
            kept.push_back(alternative);
        }
    }
}

void eraseIfUnused(Operation* op, mlir::RewriterBase& rewriter) {
    if (op && op->use_empty()) {
        rewriter.eraseOp(op);
    }
}

// A WHERE spelling a boolean of labels reaches here as one check per label, joined by AND and
// OR. Two checks over the same nodes are one check, whichever of the two joins them.
struct LabelCheckPair {
    CheckLabelConstraint _left;
    CheckLabelConstraint _right;
    GetNodeLabelSet _rightLabelSet;
};

bool matchLabelCheckPair(Value lhs, Value rhs, LabelCheckPair& pair) {
    CheckLabelConstraint left = lhs.getDefiningOp<CheckLabelConstraint>();
    CheckLabelConstraint right = rhs.getDefiningOp<CheckLabelConstraint>();
    if (!left || !right) {
        return false;
    }

    GetNodeLabelSet leftLabelSet = left.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    GetNodeLabelSet rightLabelSet = right.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    const bool overSameNodes = leftLabelSet && rightLabelSet && leftLabelSet.getInputNodes() == rightLabelSet.getInputNodes();
    if (!overSameNodes || left.getNullable() != right.getNullable()) {
        return false;
    }

    pair = LabelCheckPair {._left = left, ._right = right, ._rightLabelSet = rightLabelSet};

    return true;
}

void replaceWithLabelCheck(Operation* op,
                           const LabelCheckPair& pair,
                           llvm::ArrayRef<Attribute> alternatives,
                           mlir::PatternRewriter& rewriter) {
    CheckLabelConstraint left = pair._left;
    CheckLabelConstraint right = pair._right;
    GetNodeLabelSet rightLabelSet = pair._rightLabelSet;

    llvm::SmallVector<Attribute, 4> kept;
    keepWeakestAlternatives(alternatives, kept);

    Value result = op->getResult(0);
    CheckLabelConstraint fused = rewriter.create<CheckLabelConstraint>(op->getLoc(),
                                                                       result.getType(),
                                                                       left.getLabelsetIds(),
                                                                       rewriter.getArrayAttr(kept),
                                                                       left.getNullable());
    rewriter.replaceOp(op, fused.getResult());

    eraseIfUnused(left.getOperation(), rewriter);
    if (right != left) {
        eraseIfUnused(right.getOperation(), rewriter);
        eraseIfUnused(rightLabelSet.getOperation(), rewriter);
    }
}

struct FuseLabelDisjunctionPattern : public mlir::OpRewritePattern<OrOp> {
    using OpRewritePattern::OpRewritePattern;

    LogicalResult matchAndRewrite(OrOp disjunction, mlir::PatternRewriter& rewriter) const override {
        LabelCheckPair pair;
        if (!matchLabelCheckPair(disjunction.getLhs(), disjunction.getRhs(), pair)) {
            return failure();
        }

        const ArrayAttr leftAlternatives = pair._left.getAlternatives();
        llvm::SmallVector<Attribute, 4> alternatives(leftAlternatives.begin(), leftAlternatives.end());
        const ArrayAttr rightAlternatives = pair._right.getAlternatives();
        alternatives.append(rightAlternatives.begin(), rightAlternatives.end());

        replaceWithLabelCheck(disjunction, pair, alternatives, rewriter);

        return success();
    }
};

// The conjunction of two disjunctions has an alternative per pair of theirs, so a chain of
// them would grow exponentially: one side has to be a single conjunction.
bool conjoinAlternatives(ArrayAttr leftAlternatives,
                         ArrayAttr rightAlternatives,
                         mlir::Builder& builder,
                         llvm::SmallVectorImpl<Attribute>& alternatives) {
    if (leftAlternatives.size() > 1 && rightAlternatives.size() > 1) {
        return false;
    }

    for (const Attribute leftAlternative : leftAlternatives) {
        const ArrayAttr leftLabels = cast<ArrayAttr>(leftAlternative);

        for (const Attribute rightAlternative : rightAlternatives) {
            llvm::SmallVector<Attribute, 4> labels(leftLabels.begin(), leftLabels.end());
            for (const Attribute label : cast<ArrayAttr>(rightAlternative)) {
                if (!llvm::is_contained(labels, label)) {
                    labels.push_back(label);
                }
            }

            alternatives.push_back(builder.getArrayAttr(labels));
        }
    }

    return true;
}

struct FuseLabelConjunctionPattern : public mlir::OpRewritePattern<AndOp> {
    using OpRewritePattern::OpRewritePattern;

    LogicalResult matchAndRewrite(AndOp conjunction, mlir::PatternRewriter& rewriter) const override {
        LabelCheckPair pair;
        if (!matchLabelCheckPair(conjunction.getLhs(), conjunction.getRhs(), pair)) {
            return failure();
        }

        llvm::SmallVector<Attribute, 4> alternatives;
        if (!conjoinAlternatives(pair._left.getAlternatives(), pair._right.getAlternatives(), rewriter, alternatives)) {
            return failure();
        }

        replaceWithLabelCheck(conjunction, pair, alternatives, rewriter);

        return success();
    }
};

// Codegen filters once per conjunct, so a WHERE ANDing labels reaches here as a label filter
// over the rows of another one, testing the same nodes.
struct StackedLabelFilters {
    FilterOp _outer;
    FilterOp _inner;
    CheckLabelConstraint _innerCheck;
    CheckLabelConstraint _outerCheck;
    GetNodeLabelSet _outerLabelSet;
    llvm::SmallVector<Attribute, 4> _alternatives;
};

bool matchStackedLabelFilters(FilterOp outer, StackedLabelFilters& stacked) {
    CheckLabelConstraint outerCheck = outer.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!outerCheck) {
        return false;
    }

    GetNodeLabelSet outerLabelSet = outerCheck.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!outerLabelSet) {
        return false;
    }

    const Value checkedColumn = outerLabelSet.getInputNodes();
    FilterOp inner = checkedColumn.getDefiningOp<FilterOp>();
    if (!inner) {
        return false;
    }

    CheckLabelConstraint innerCheck = inner.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!innerCheck) {
        return false;
    }

    GetNodeLabelSet innerLabelSet = innerCheck.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    const size_t checkedIndex = cast<OpResult>(checkedColumn).getResultNumber();
    const bool overSameNodes = innerLabelSet && innerLabelSet.getInputNodes() == inner.getColumnsToFilter()[checkedIndex];
    const bool inSameBlock = outer->getBlock() == inner->getBlock();
    if (!overSameNodes || !inSameBlock) {
        return false;
    }

    for (const Value column : outer.getColumnsToFilter()) {
        if (column.getDefiningOp() != inner.getOperation()) {
            return false;
        }
    }

    // Moving the outer check onto the inner filter cuts the inner filter's rows, so nothing
    // but the outer filter and its check may read them.
    Operation* const outerOp = outer.getOperation();
    Operation* const outerLabelSetOp = outerLabelSet.getOperation();
    for (const Value result : inner->getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsTheChain = user == outerOp || user == outerLabelSetOp;
            if (!readsTheChain) {
                return false;
            }
        }
    }

    const bool chainIsPrivate = outerLabelSet.getResult().hasOneUse() && outerCheck.getResult().hasOneUse();
    if (!chainIsPrivate) {
        return false;
    }

    mlir::Builder builder(outer.getContext());
    llvm::SmallVector<Attribute, 4> alternatives;
    if (!conjoinAlternatives(innerCheck.getAlternatives(), outerCheck.getAlternatives(), builder, alternatives)) {
        return false;
    }

    stacked._outer = outer;
    stacked._inner = inner;
    stacked._innerCheck = innerCheck;
    stacked._outerCheck = outerCheck;
    stacked._outerLabelSet = outerLabelSet;
    keepWeakestAlternatives(alternatives, stacked._alternatives);

    return true;
}

void fuseStackedLabelFilters(StackedLabelFilters& stacked, mlir::RewriterBase& rewriter) {
    FilterOp inner = stacked._inner;
    CheckLabelConstraint innerCheck = stacked._innerCheck;
    const bool nullable = innerCheck.getNullable() || stacked._outerCheck.getNullable();

    rewriter.setInsertionPoint(inner);
    CheckLabelConstraint fused = rewriter.create<CheckLabelConstraint>(innerCheck.getLoc(),
                                                                       innerCheck.getResult().getType(),
                                                                       innerCheck.getLabelsetIds(),
                                                                       rewriter.getArrayAttr(stacked._alternatives),
                                                                       nullable);

    rewriter.modifyOpInPlace(inner, [&inner, &fused]() {
        inner.getMaskMutable().assign(fused.getResult());
    });

    FilterOp outer = stacked._outer;
    rewriter.replaceOp(outer, outer.getColumnsToFilter());

    eraseIfUnused(stacked._outerCheck.getOperation(), rewriter);
    eraseIfUnused(stacked._outerLabelSet.getOperation(), rewriter);
    eraseIfUnused(innerCheck.getOperation(), rewriter);
}

// A hop repeats each node it walks from, and each column it carries, once per edge, so a label
// filter over one of those columns keeps the rows of the nodes it would keep above the hop.
struct LabelFilterBelowHop {
    FilterOp _filter;
    Operation* _hop {nullptr};
    CheckLabelConstraint _check;
    GetNodeLabelSet _labelSet;
    Value _hopOperand;
};

bool matchLabelFilterBelowHop(FilterOp filter, LabelFilterBelowHop& below) {
    CheckLabelConstraint check = filter.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet) {
        return false;
    }

    const Value checkedColumn = labelSet.getInputNodes();
    Operation* const hop = checkedColumn.getDefiningOp();
    if (!hop || !isEdgeHop(hop) || hop->getBlock() != filter->getBlock()) {
        return false;
    }

    const size_t resultIndex = cast<OpResult>(checkedColumn).getResultNumber();
    Value hopOperand;
    if (resultIndex == hopInputResult(hop)) {
        hopOperand = hop->getOperand(0);
    } else if (resultIndex >= hopFixedResultCount) {
        hopOperand = hop->getOperand(1 + (resultIndex - hopFixedResultCount));
    } else {
        return false;
    }

    for (const Value column : filter.getColumnsToFilter()) {
        if (column.getDefiningOp() != hop) {
            return false;
        }
    }

    Operation* const filterOp = filter.getOperation();
    Operation* const labelSetOp = labelSet.getOperation();
    for (const Value result : hop->getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsTheChain = user == filterOp || user == labelSetOp;
            if (!readsTheChain) {
                return false;
            }
        }
    }

    const bool chainIsPrivate = labelSet.getResult().hasOneUse() && check.getResult().hasOneUse();
    if (!chainIsPrivate) {
        return false;
    }

    below = LabelFilterBelowHop {._filter = filter,
                                 ._hop = hop,
                                 ._check = check,
                                 ._labelSet = labelSet,
                                 ._hopOperand = hopOperand};

    return true;
}

void hoistLabelFilterAboveHop(LabelFilterBelowHop& below, mlir::RewriterBase& rewriter) {
    Operation* const hop = below._hop;
    CheckLabelConstraint check = below._check;
    GetNodeLabelSet labelSet = below._labelSet;
    const Location loc = check.getLoc();

    rewriter.setInsertionPoint(hop);
    GetNodeLabelSet hoistedLabelSet = rewriter.create<GetNodeLabelSet>(loc,
                                                                       labelSet.getResult().getType(),
                                                                       below._hopOperand);
    CheckLabelConstraint hoistedCheck = rewriter.create<CheckLabelConstraint>(loc,
                                                                              check.getResult().getType(),
                                                                              hoistedLabelSet.getResult(),
                                                                              check.getAlternatives(),
                                                                              check.getNullable());

    const Operation::operand_range hopOperands = hop->getOperands();
    FilterOp hoistedFilter = rewriter.create<FilterOp>(loc,
                                                       hopOperands.getTypes(),
                                                       hoistedCheck.getResult(),
                                                       hopOperands);

    const mlir::ResultRange filteredOperands = hoistedFilter.getFilteredColumns();
    rewriter.modifyOpInPlace(hop, [hop, &filteredOperands]() {
        hop->setOperands(filteredOperands);
    });

    FilterOp filter = below._filter;
    rewriter.replaceOp(filter, filter.getColumnsToFilter());

    eraseIfUnused(check.getOperation(), rewriter);
    eraseIfUnused(labelSet.getOperation(), rewriter);
}

struct FuseLabelPredicates : public impl::FuseLabelPredicatesBase<FuseLabelPredicates> {
    void runOnOperation() override {
        MLIRContext* const context = &getContext();
        Operation* const root = getOperation();

        llvm::SmallVector<Operation*> predicates;
        root->walk([&predicates](Operation* op) {
            if (isa<AndOp, OrOp>(op)) {
                predicates.push_back(op);
            }
        });

        mlir::RewritePatternSet patterns(context);
        patterns.add<FuseLabelDisjunctionPattern, FuseLabelConjunctionPattern>(context);
        const mlir::FrozenRewritePatternSet frozenPatterns(std::move(patterns));

        // Kept to the ANDs, the ORs and the checks fused from them: the driver's folding and
        // region simplification would otherwise rewrite the rest of the program as well.
        mlir::GreedyRewriteConfig config;
        config.setStrictness(mlir::GreedyRewriteStrictness::ExistingAndNewOps);
        config.enableFolding(false);
        config.enableConstantCSE(false);
        config.setRegionSimplificationLevel(mlir::GreedySimplifyRegionLevel::Disabled);

        if (failed(mlir::applyOpPatternsGreedily(predicates, frozenPatterns, config))) {
            signalPassFailure();
            return;
        }

        runFilterWorklist<LabelFilterBelowHop>(root, matchLabelFilterBelowHop, hoistLabelFilterAboveHop);
        runFilterWorklist<StackedLabelFilters>(root, matchStackedLabelFilters, fuseStackedLabelFilters);
    }
};

// A type check over a read that already narrows by type is that read's types ANDed with the
// check's: the rows that survive are the ones both keep. Folding the check into the read
// leaves the walk to skip everything else, and the check and its filter go. A check the read
// already guarantees is the case where the intersection changes nothing, so only the check
// goes.
struct TypeCheckOverRead {
    CheckEdgeTypeConstraint _check;
    Operation* _read {nullptr};
    llvm::SmallVector<Attribute, 4> _intersection;
    bool _narrowsTheRead {false};
};

// The by-type read a column comes off, or null for a column no read constrains. The by-type
// reads declare four columns and only the edge type one is constrained, so which result the
// value is matters as much as which op produced it.
Operation* byTypeReadOf(Value column) {
    Operation* const producer = column.getDefiningOp();
    if (!producer || !isa<ScanEdgesByType, GetOutEdgesByType, GetInEdgesByType, GetOutEdgesByTypeAndLabel, GetInEdgesByTypeAndLabel>(producer)) {
        return nullptr;
    }

    constexpr size_t etypesResultIndex = 2;
    if (column != producer->getResult(etypesResultIndex)) {
        return nullptr;
    }

    return producer;
}

// Narrowing a read changes what every reader of it sees, so it is only safe when the check
// and the filter over it are the only ones.
bool readOnlyFeeds(Operation* read, CheckEdgeTypeConstraint check, FilterOp filter) {
    Operation* const checkOp = check.getOperation();
    Operation* const filterOp = filter.getOperation();

    for (const Value result : read->getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool feedsThePair = user == checkOp || user == filterOp;
            if (!feedsThePair) {
                return false;
            }
        }
    }

    return true;
}

// Every column the filter cuts has to be one the read bound, or narrowing the read shortens
// its own columns while the carried one comes back whole.
bool readBindsEveryColumn(Operation* read, FilterOp filter) {
    for (const Value column : filter.getColumnsToFilter()) {
        if (column.getDefiningOp() != read) {
            return false;
        }
    }

    return true;
}

bool matchTypeCheckOverRead(FilterOp filter, TypeCheckOverRead& matched) {
    CheckEdgeTypeConstraint check = filter.getMask().getDefiningOp<CheckEdgeTypeConstraint>();
    if (!check) {
        return false;
    }

    Operation* const read = byTypeReadOf(check.getEdgeTypeIds());
    if (!read) {
        return false;
    }

    const ArrayAttr readTypes = read->getAttrOfType<ArrayAttr>("edge_types");
    combineEdgeTypes(false, readTypes, check.getEdgeTypes(), matched._intersection);

    // An empty intersection is a read that matches no edge, which an empty type set on the
    // read says outright: the loop is then unmatchable and walks nothing.
    // The intersection is a subset of the read's types, so a smaller one is a narrower read
    matched._narrowsTheRead = matched._intersection.size() < readTypes.size();

    const bool checkIsPrivate = check.getResult().hasOneUse();
    const bool columnsAreTheReads = readBindsEveryColumn(read, filter);
    const bool narrowingIsSafe = checkIsPrivate && columnsAreTheReads && readOnlyFeeds(read, check, filter);
    if (matched._narrowsTheRead && !narrowingIsSafe) {
        return false;
    }

    matched._check = check;
    matched._read = read;

    return true;
}

void foldTypeCheckIntoRead(FilterOp filter, const TypeCheckOverRead& matched, mlir::OpBuilder& builder) {
    if (matched._narrowsTheRead) {
        matched._read->setAttr("edge_types", builder.getArrayAttr(matched._intersection));
    }

    // The read now emits only the rows the check kept, so each column the filter handed on is
    // the one it was given.
    const Operation::operand_range columns = filter.getColumnsToFilter();
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    filter.erase();
    CheckEdgeTypeConstraint check = matched._check;
    eraseIfUnused(check.getOperation());
}

struct NarrowEdgeTypeReads : public impl::NarrowEdgeTypeReadsBase<NarrowEdgeTypeReads> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<TypeCheckOverRead>(getOperation(), matchTypeCheckOverRead, foldTypeCheckIntoRead, builder);
    }
};

// A directed hop over a by-label node scan walks the edges hanging off every node carrying
// those labels, which is what the matching by-label edge scan reads off the edge index -
// without building the node column the hop walks from.
template <typename HopOp>
bool matchLabelledEdgeScan(HopOp hop, ScanNodesByLabel& scan) {
    // The carry set is row-aligned with the scan and the fused form has nothing of its own
    // to hand back in its place.
    if (!hop.getColumnsToFilter().empty()) {
        return false;
    }

    scan = hop.getInputNodes().template getDefiningOp<ScanNodesByLabel>();
    if (!scan) {
        return false;
    }

    // The fused form drops the node column, so a second reader of it keeps the scan alive.
    return scan.getResult().hasOneUse();
}

template <typename ScanOp, typename HopOp>
void fuseScanEdgesByLabel(HopOp hop, ScanNodesByLabel scan, mlir::OpBuilder& builder) {
    builder.setInsertionPoint(hop);

    // The fused scan declares the same four results in the same order the hop declares its
    // fixed ones, so they map one for one. Both hold the edge as the graph stores it, so
    // the direction changes which edges are produced, not which column holds what.
    ScanOp edgeScan = builder.create<ScanOp>(hop.getLoc(),
                                             hop.getSrcids().getType(),
                                             hop.getEids().getType(),
                                             hop.getEtypes().getType(),
                                             hop.getTgtids().getType(),
                                             scan.getLabelsAttr());

    Operation* const hopOp = hop.getOperation();
    hopOp->replaceAllUsesWith(edgeScan.getOperation());
    hopOp->erase();
    scan.erase();
}

template <typename ScanOp, typename HopOp>
void runScanEdgesByLabelPass(Operation* root, mlir::OpBuilder& builder) {
    // Collect first: fusing erases the hop and its scan, which would invalidate the walk.
    llvm::SmallVector<HopOp> hops;
    root->walk([&](HopOp hop) {
        ScanNodesByLabel scan;
        if (matchLabelledEdgeScan(hop, scan)) {
            hops.push_back(hop);
        }
    });

    for (HopOp hop : hops) {
        ScanNodesByLabel scan;
        if (!matchLabelledEdgeScan(hop, scan)) {
            continue;
        }

        fuseScanEdgesByLabel<ScanOp>(hop, scan, builder);
    }
}

struct FuseScanOutEdgesByLabel : public impl::FuseScanOutEdgesByLabelBase<FuseScanOutEdgesByLabel> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runScanEdgesByLabelPass<ScanOutEdgesByLabelSrc, GetOutEdges>(getOperation(), builder);
    }
};

struct FuseScanInEdgesByLabel : public impl::FuseScanInEdgesByLabelBase<FuseScanInEdgesByLabel> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runScanEdgesByLabelPass<ScanInEdgesByLabelTgt, GetInEdges>(getOperation(), builder);
    }
};

// An edge scan whose rows are then cut down to the edges one endpoint of which carries a
// set of labels: the by-label edge scan spelled the long way, since the index keyed by that
// endpoint's label set holds exactly those edges and never builds the rows the filter goes
// on to drop.
struct EndpointLabelledEdgeScan {
    ScanEdges _scan;
    GetNodeLabelSet _labelSet;
    CheckLabelConstraint _check;
    ArrayAttr _labels;
    bool _labelledTarget {false};
};

bool matchEndpointLabelledEdgeScan(FilterOp filter, EndpointLabelledEdgeScan& labelledScan) {
    CheckLabelConstraint check = filter.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return false;
    }

    const ArrayAttr labels = check.getConjunction();
    if (!labels) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet) {
        return false;
    }

    const Value labelledColumn = labelSet.getInputNodes();
    ScanEdges scan = labelledColumn.getDefiningOp<ScanEdges>();
    if (!scan) {
        return false;
    }

    const bool labelledTarget = labelledColumn == scan.getTgtids();
    const bool labelledSource = labelledColumn == scan.getSrcids();
    if (!labelledTarget && !labelledSource) {
        return false;
    }

    for (const Value column : filter.getColumnsToFilter()) {
        if (column.getDefiningOp() != scan.getOperation()) {
            return false;
        }
    }

    Operation* const filterOp = filter.getOperation();
    Operation* const labelSetOp = labelSet.getOperation();
    for (const Value result : scan->getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsTheChain = user == filterOp || user == labelSetOp;
            if (!readsTheChain) {
                return false;
            }
        }
    }

    const bool chainIsPrivate = labelSet.getResult().hasOneUse() && check.getResult().hasOneUse();
    if (!chainIsPrivate) {
        return false;
    }

    labelledScan = EndpointLabelledEdgeScan {._scan = scan,
                                             ._labelSet = labelSet,
                                             ._check = check,
                                             ._labels = labels,
                                             ._labelledTarget = labelledTarget};

    return true;
}

void fuseScanEdgesByEndpointLabel(FilterOp filter,
                                  const EndpointLabelledEdgeScan& labelledScan,
                                  mlir::OpBuilder& builder) {
    ScanEdges scan = labelledScan._scan;
    Operation* const scanOp = scan.getOperation();

    builder.setInsertionPoint(scanOp);

    // Both by-label scans declare the same four results in the same order as the plain
    // scan, so the plain scan's map onto them one for one. Which of the two the labelled
    // endpoint picks is the hop the query wrote: an out-hop arrives at its target, an
    // in-hop leaves its source.
    Operation* byLabelScan = nullptr;
    if (labelledScan._labelledTarget) {
        byLabelScan = builder.create<ScanOutEdgesByLabelTgt>(scan.getLoc(),
                                                             scan.getSrcids().getType(),
                                                             scan.getEids().getType(),
                                                             scan.getEtypes().getType(),
                                                             scan.getTgtids().getType(),
                                                             labelledScan._labels);
    } else {
        byLabelScan = builder.create<ScanInEdgesByLabelSrc>(scan.getLoc(),
                                                            scan.getSrcids().getType(),
                                                            scan.getEids().getType(),
                                                            scan.getEtypes().getType(),
                                                            scan.getTgtids().getType(),
                                                            labelledScan._labels);
    }

    scanOp->replaceAllUsesWith(byLabelScan);

    const Operation::operand_range columns = filter.getColumnsToFilter();
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    filter.erase();
    eraseIfUnused(labelledScan._check);
    eraseIfUnused(labelledScan._labelSet);
    scanOp->erase();
}

struct FuseScanEdgesByEndpointLabel : public impl::FuseScanEdgesByEndpointLabelBase<FuseScanEdgesByEndpointLabel> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<EndpointLabelledEdgeScan>(getOperation(),
                                                matchEndpointLabelledEdgeScan,
                                                fuseScanEdgesByEndpointLabel,
                                                builder);
    }
};

// A hop whose rows are then cut down to the edges the endpoint it reaches carries a set of
// labels on: the by-label hop spelled the long way, since the walk itself can keep those
// edges and never build the rows the filter goes on to drop.
struct EndpointLabelledHop {
    Operation* _hop {nullptr};
    GetNodeLabelSet _labelSet;
    CheckLabelConstraint _check;
    ArrayAttr _labels;
};

bool matchEndpointLabelledHop(FilterOp filter, EndpointLabelledHop& labelledHop) {
    CheckLabelConstraint check = filter.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return false;
    }

    const ArrayAttr labels = check.getConjunction();
    if (!labels) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet) {
        return false;
    }

    const Value labelledColumn = labelSet.getInputNodes();
    Operation* const hop = labelledColumn.getDefiningOp();
    if (!hop || !isa<GetOutEdges, GetInEdges, GetOutEdgesByType, GetInEdgesByType>(hop)) {
        return false;
    }

    // Only the end the hop reaches can ride onto it. The end it leaves is the input column,
    // which a by-label node scan constrains instead - and there the labels are no longer
    // this hop's to carry.
    constexpr size_t srcResultIndex = 0;
    constexpr size_t tgtResultIndex = 3;
    const size_t reachedResultIndex = isReverseHop(hop) ? srcResultIndex : tgtResultIndex;
    if (labelledColumn != hop->getResult(reachedResultIndex)) {
        return false;
    }

    // Every column the filter cuts has to be one the hop bound, or the fused hop has nothing
    // of its own to hand back in its place.
    for (const Value column : filter.getColumnsToFilter()) {
        if (column.getDefiningOp() != hop) {
            return false;
        }
    }

    // And nothing outside the chain may read the hop, or that reader would go on seeing the
    // rows the labels turn away.
    Operation* const filterOp = filter.getOperation();
    Operation* const labelSetOp = labelSet.getOperation();
    for (const Value result : hop->getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsTheChain = user == filterOp || user == labelSetOp;
            if (!readsTheChain) {
                return false;
            }
        }
    }

    const bool chainIsPrivate = labelSet.getResult().hasOneUse() && check.getResult().hasOneUse();
    if (!chainIsPrivate) {
        return false;
    }

    labelledHop = EndpointLabelledHop {._hop = hop,
                                       ._labelSet = labelSet,
                                       ._check = check,
                                       ._labels = labels};

    return true;
}

template <typename ByLabelOp>
Operation* createByLabelHop(Operation* hop, ArrayAttr labels, mlir::OpBuilder& builder) {
    const Operation::result_range results = hop->getResults();

    ByLabelOp byLabelHop = builder.create<ByLabelOp>(hop->getLoc(),
                                                     results[0].getType(),
                                                     results[1].getType(),
                                                     results[2].getType(),
                                                     results[3].getType(),
                                                     results.drop_front(hopFixedResultCount).getTypes(),
                                                     hop->getOperand(0),
                                                     labels,
                                                     hop->getOperands().drop_front());

    return byLabelHop.getOperation();
}

template <typename ByTypeAndLabelOp>
Operation* createByTypeAndLabelHop(Operation* hop, ArrayAttr labels, mlir::OpBuilder& builder) {
    const Operation::result_range results = hop->getResults();
    const ArrayAttr edgeTypes = hop->getAttrOfType<ArrayAttr>("edge_types");

    ByTypeAndLabelOp byTypeAndLabelHop = builder.create<ByTypeAndLabelOp>(hop->getLoc(),
                                                                          results[0].getType(),
                                                                          results[1].getType(),
                                                                          results[2].getType(),
                                                                          results[3].getType(),
                                                                          results.drop_front(hopFixedResultCount).getTypes(),
                                                                          hop->getOperand(0),
                                                                          edgeTypes,
                                                                          labels,
                                                                          hop->getOperands().drop_front());

    return byTypeAndLabelHop.getOperation();
}

Operation* createEndpointLabelledHop(Operation* hop, ArrayAttr labels, mlir::OpBuilder& builder) {
    if (isa<GetOutEdges>(hop)) {
        return createByLabelHop<GetOutEdgesByLabel>(hop, labels, builder);
    } else if (isa<GetInEdges>(hop)) {
        return createByLabelHop<GetInEdgesByLabel>(hop, labels, builder);
    } else if (isa<GetOutEdgesByType>(hop)) {
        return createByTypeAndLabelHop<GetOutEdgesByTypeAndLabel>(hop, labels, builder);
    } else {
        return createByTypeAndLabelHop<GetInEdgesByTypeAndLabel>(hop, labels, builder);
    }
}

void fuseEdgesByEndpointLabel(FilterOp filter, const EndpointLabelledHop& labelledHop, mlir::OpBuilder& builder) {
    Operation* const hop = labelledHop._hop;

    builder.setInsertionPoint(hop);

    // A by-label hop declares the same four fixed results and the same carry set behind
    // them, so the plain hop's results map onto it one for one. Which one it becomes is the
    // hop the query wrote: an out-hop reaches its target, an in-hop its source, and a by-type
    // hop keeps its types.
    Operation* const byLabelHop = createEndpointLabelledHop(hop, labelledHop._labels, builder);

    hop->replaceAllUsesWith(byLabelHop);

    // The hop now yields the rows the filter used to leave, so each column the filter handed
    // on is the one it was given.
    const Operation::operand_range columns = filter.getColumnsToFilter();
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    filter.erase();
    eraseIfUnused(labelledHop._check);
    eraseIfUnused(labelledHop._labelSet);
    hop->erase();
}

struct FuseEdgesByEndpointLabel : public impl::FuseEdgesByEndpointLabelBase<FuseEdgesByEndpointLabel> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<EndpointLabelledHop>(getOperation(),
                                           matchEndpointLabelledHop,
                                           fuseEdgesByEndpointLabel,
                                           builder);
    }
};

void addLabelNames(ArrayAttr names, llvm::StringSet<>& labels) {
    for (const Attribute name : names) {
        labels.insert(cast<StringAttr>(name).getValue());
    }
}

void addSharedLabelNames(ArrayAttr alternatives, llvm::StringSet<>& labels) {
    for (const Attribute label : cast<ArrayAttr>(alternatives[0])) {
        const auto asksFor = [label](Attribute alternative) {
            return llvm::is_contained(cast<ArrayAttr>(alternative), label);
        };

        if (llvm::all_of(alternatives, asksFor)) {
            labels.insert(cast<StringAttr>(label).getValue());
        }
    }
}

void addFilterLabels(FilterOp filter, Value filtered, llvm::StringSet<>& labels) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    for (const Value conjunct : conjuncts) {
        CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
        if (!check) {
            continue;
        }

        GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
        if (labelSet && labelSet.getInputNodes() == filtered) {
            addSharedLabelNames(check.getAlternatives(), labels);
        }
    }
}

// The labels every node of a column carries, known from where its rows come from: the
// by-label read that made them and each label filter they passed on the way.
void collectKnownLabels(Value column, llvm::StringSet<>& labels) {
    constexpr size_t srcResultIndex = 0;
    constexpr size_t tgtResultIndex = 3;

    for (;;) {
        Operation* const def = column.getDefiningOp();
        if (!def) {
            return;
        }

        const size_t resultIndex = cast<OpResult>(column).getResultNumber();

        if (ScanNodesByLabel scan = dyn_cast<ScanNodesByLabel>(def)) {
            addLabelNames(scan.getLabels(), labels);
            return;
        } else if (ScanNodesByPropertyValue scan = dyn_cast<ScanNodesByPropertyValue>(def)) {
            if (const std::optional<ArrayAttr> scanLabels = scan.getLabels()) {
                addLabelNames(*scanLabels, labels);
            }
            return;
        } else if (isa<ScanOutEdgesByLabelSrc, ScanInEdgesByLabelSrc>(def)) {
            if (resultIndex == srcResultIndex) {
                addLabelNames(def->getAttrOfType<ArrayAttr>("labels"), labels);
            }
            return;
        } else if (isa<ScanOutEdgesByLabelTgt, ScanInEdgesByLabelTgt>(def)) {
            if (resultIndex == tgtResultIndex) {
                addLabelNames(def->getAttrOfType<ArrayAttr>("labels"), labels);
            }
            return;
        } else if (FilterOp filter = dyn_cast<FilterOp>(def)) {
            const Value filtered = filter.getColumnsToFilter()[resultIndex];
            addFilterLabels(filter, filtered, labels);
            column = filtered;
        } else if (isEdgeHop(def)) {
            const size_t inputResultIndex = isReverseHop(def) ? tgtResultIndex : srcResultIndex;
            const size_t reachedResultIndex = isReverseHop(def) ? srcResultIndex : tgtResultIndex;

            if (resultIndex == inputResultIndex) {
                column = def->getOperand(0);
            } else if (resultIndex >= hopFixedResultCount) {
                column = def->getOperand(1 + (resultIndex - hopFixedResultCount));
            } else {
                const ArrayAttr hopLabels = def->getAttrOfType<ArrayAttr>("labels");
                if (resultIndex == reachedResultIndex && hopLabels) {
                    addLabelNames(hopLabels, labels);
                }
                return;
            }
        } else {
            return;
        }
    }
}

// A label filter over nodes already known to carry every label of one of its alternatives
// keeps every row.
struct RedundantLabelCheck {
    CheckLabelConstraint _check;
    GetNodeLabelSet _labelSet;
};

bool matchRedundantLabelCheck(FilterOp filter, RedundantLabelCheck& redundant) {
    CheckLabelConstraint check = filter.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet) {
        return false;
    }

    llvm::StringSet<> knownLabels;
    collectKnownLabels(labelSet.getInputNodes(), knownLabels);

    const auto isKnown = [&knownLabels](Attribute label) {
        return knownLabels.contains(cast<StringAttr>(label).getValue());
    };

    const auto isGuaranteed = [&isKnown](Attribute alternative) {
        return llvm::all_of(cast<ArrayAttr>(alternative), isKnown);
    };

    if (!llvm::any_of(check.getAlternatives(), isGuaranteed)) {
        return false;
    }

    redundant = RedundantLabelCheck {._check = check, ._labelSet = labelSet};

    return true;
}

void removeRedundantLabelCheck(FilterOp filter, const RedundantLabelCheck& redundant, mlir::OpBuilder& builder) {
    const Operation::operand_range columns = filter.getColumnsToFilter();
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    filter.erase();
    eraseIfUnused(redundant._check);
    eraseIfUnused(redundant._labelSet);
}

struct RemoveRedundantLabelChecks : public impl::RemoveRedundantLabelChecksBase<RemoveRedundantLabelChecks> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<RedundantLabelCheck>(getOperation(),
                                           matchRedundantLabelCheck,
                                           removeRedundantLabelCheck,
                                           builder);
    }
};

// A path exploration whose rows are then cut down to those ending on labelled nodes: the
// end constraint spelled the long way, since the walk itself can keep to those ends and
// never build the rows the filter goes on to drop.
struct EndConstrainedExploration {
    ExplorePaths _exploration;
    GetNodeLabelSet _labelSet;
    CheckLabelConstraint _check;
    ArrayAttr _labels;
};

bool matchEndConstrainedExploration(FilterOp filter, EndConstrainedExploration& constrained) {
    CheckLabelConstraint check = filter.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return false;
    }

    const ArrayAttr labels = check.getConjunction();
    if (!labels) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet) {
        return false;
    }

    const Value ends = labelSet.getInputNodes();
    ExplorePaths exploration = ends.getDefiningOp<ExplorePaths>();
    if (!exploration || ends != exploration.getTgtids()) {
        return false;
    }

    // Every column the filter cuts has to be one the exploration bound, or the constrained
    // exploration has nothing of its own to hand back in its place.
    for (const Value column : filter.getColumnsToFilter()) {
        if (column.getDefiningOp() != exploration.getOperation()) {
            return false;
        }
    }

    // And nothing outside the trio may read the exploration, or that reader would go on
    // seeing the rows the end labels turn away.
    for (const Value result : exploration.getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsTheTrio = user == filter.getOperation() || user == labelSet.getOperation();
            if (!readsTheTrio) {
                return false;
            }
        }
    }

    constrained = EndConstrainedExploration {._exploration = exploration,
                                             ._labelSet = labelSet,
                                             ._check = check,
                                             ._labels = labels};

    return true;
}

// The end labels the exploration already carries joined with the filter's, each once
ArrayAttr mergedEndLabels(ExplorePaths exploration, ArrayAttr filterLabels, mlir::OpBuilder& builder) {
    const std::optional<ArrayAttr> current = exploration.getEndLabels();
    if (!current) {
        return filterLabels;
    }

    llvm::SmallVector<Attribute> merged(current->begin(), current->end());
    for (const Attribute label : filterLabels) {
        if (!llvm::is_contained(merged, label)) {
            merged.push_back(label);
        }
    }

    return builder.getArrayAttr(merged);
}

void fuseExploreEndConstraint(FilterOp filter, const EndConstrainedExploration& constrained, mlir::OpBuilder& builder) {
    ExplorePaths exploration = constrained._exploration;
    GetNodeLabelSet labelSet = constrained._labelSet;
    CheckLabelConstraint check = constrained._check;

    exploration.setEndLabelsAttr(mergedEndLabels(exploration, constrained._labels, builder));

    // The exploration now yields the rows the filter used to leave, so each column the
    // filter handed on is the one it was given.
    const Operation::operand_range columns = filter.getColumnsToFilter();
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    filter.erase();
    eraseIfUnused(check);
    eraseIfUnused(labelSet);
}

struct FuseExploreEndConstraint : public impl::FuseExploreEndConstraintBase<FuseExploreEndConstraint> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<EndConstrainedExploration>(getOperation(), matchEndConstrainedExploration, fuseExploreEndConstraint, builder);
    }
};

Type boolColumnType(mlir::MLIRContext* context) {
    return ColumnType::get(context, storage::BoolType::get(context));
}

// A conjunct of a hop region's yield that only asks the hop's end node for labels
CheckLabelConstraint endLabelCheckOf(Value conjunct, BlockArgument end) {
    CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
    if (!check || !conjunct.hasOneUse()) {
        return {};
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    const bool readsTheEnd = labelSet && labelSet.getInputNodes() == end;
    const bool isConjunction = static_cast<bool>(check.getConjunction());

    return readsTheEnd && isConjunction ? check : CheckLabelConstraint {};
}

// The conjuncts of a hop region's yield, and the `and` ops joining them, each read by its
// parent alone, listed parent first
void collectHopConjuncts(Value mask, llvm::SmallVectorImpl<Value>& conjuncts, llvm::SmallVectorImpl<AndOp>& joins) {
    llvm::SmallVector<Value> pending {mask};
    while (!pending.empty()) {
        const Value conjunct = pending.pop_back_val();

        AndOp conjunction = conjunct.getDefiningOp<AndOp>();
        if (conjunction && conjunct.hasOneUse()) {
            joins.push_back(conjunction);
            pending.push_back(conjunction.getRhs());
            pending.push_back(conjunction.getLhs());
        } else {
            conjuncts.push_back(conjunct);
        }
    }
}

// The labels a hop region asks of the hop's end node become hop_labels. The region keeps its
// other conjuncts, and goes when there is none left.
void fuseHopLabels(ExplorePaths exploration, mlir::OpBuilder& builder) {
    Region& hop = exploration.getHop();
    if (hop.empty()) {
        return;
    }

    Block& block = hop.front();
    Yield yield = dyn_cast<Yield>(block.getTerminator());
    if (!yield || yield->getNumOperands() != 1) {
        return;
    }

    llvm::SmallVector<Value> conjuncts;
    llvm::SmallVector<AndOp> joins;
    collectHopConjuncts(yield->getOperand(0), conjuncts, joins);

    const BlockArgument end = block.getArgument(2);

    llvm::SmallVector<CheckLabelConstraint> checks;
    llvm::SmallVector<Value> residual;
    for (const Value conjunct : conjuncts) {
        if (CheckLabelConstraint check = endLabelCheckOf(conjunct, end)) {
            checks.push_back(check);
        } else {
            residual.push_back(conjunct);
        }
    }

    if (checks.empty()) {
        return;
    }

    llvm::SmallVector<Attribute> labels;
    if (const std::optional<ArrayAttr> current = exploration.getHopLabels()) {
        labels.append(current->begin(), current->end());
    }

    for (CheckLabelConstraint check : checks) {
        for (const Attribute label : check.getConjunction()) {
            if (!llvm::is_contained(labels, label)) {
                labels.push_back(label);
            }
        }
    }

    exploration.setHopLabelsAttr(builder.getArrayAttr(labels));

    if (residual.empty()) {
        exploration.getHopImportsMutable().clear();
        hop.dropAllReferences();
        hop.getBlocks().clear();
        return;
    }

    builder.setInsertionPoint(yield);
    const Type boolType = boolColumnType(builder.getContext());

    Value mask = residual.front();
    for (const Value conjunct : llvm::drop_begin(residual)) {
        mask = builder.create<AndOp>(yield.getLoc(), boolType, mask, conjunct).getResult();
    }

    yield->setOperand(0, mask);

    for (AndOp join : joins) {
        join.erase();
    }

    for (CheckLabelConstraint check : checks) {
        Operation* const labelSet = check.getLabelsetIds().getDefiningOp();
        check.erase();
        eraseIfUnused(labelSet);
    }
}

struct FuseExploreHopLabels : public impl::FuseExploreHopLabelsBase<FuseExploreHopLabels> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        getOperation()->walk([&builder](ExplorePaths exploration) {
            fuseHopLabels(exploration, builder);
        });
    }
};

// A filter carrying a named path its mask does not read cuts the rows before the path is
// built: the build moves behind it, over the columns the filter keeps. Null where it stays.
MakePath sinkPastFilter(MakePath build, mlir::OpBuilder& builder) {
    const Value path = build.getResult();
    if (!path.hasOneUse()) {
        return nullptr;
    }

    FilterOp filter = dyn_cast<FilterOp>(*path.getUsers().begin());
    if (!filter || filter->getBlock() != build->getBlock()) {
        return nullptr;
    }

    const OperandRange carried = filter.getColumnsToFilter();

    llvm::SmallVector<Value> keptColumns;
    llvm::SmallVector<Type> keptTypes;
    llvm::SmallVector<size_t> keptIndices(carried.size(), 0);
    size_t pathIndex = 0;

    for (size_t index = 0; index < carried.size(); index++) {
        const Value column = carried[index];
        if (column == path) {
            pathIndex = index;
            continue;
        }

        keptIndices[index] = keptColumns.size();
        keptColumns.push_back(column);
        keptTypes.push_back(column.getType());
    }

    llvm::SmallVector<size_t> entityIndices;
    for (const Value entity : build.getEntities()) {
        const auto keptIt = llvm::find(keptColumns, entity);
        if (keptIt == keptColumns.end()) {
            return nullptr;
        }

        entityIndices.push_back(static_cast<size_t>(keptIt - keptColumns.begin()));
    }

    builder.setInsertionPoint(filter);
    FilterOp narrowed = builder.create<FilterOp>(filter.getLoc(), keptTypes, filter.getMask(), keptColumns);

    llvm::SmallVector<Value> entities;
    for (const size_t entityIndex : entityIndices) {
        entities.push_back(narrowed.getResult(entityIndex));
    }

    builder.setInsertionPointAfter(narrowed);
    MakePath sunk = builder.create<MakePath>(build.getLoc(), path.getType(), entities, build.getReversedPathsAttr());

    const ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        const Value replacement = index == pathIndex ? sunk.getResult() : narrowed.getResult(keptIndices[index]);
        filtered[index].replaceAllUsesWith(replacement);
    }

    filter.erase();
    build.erase();

    return sunk;
}

struct SinkMakePath : public impl::SinkMakePathBase<SinkMakePath> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<MakePath> builds;
        getOperation()->walk([&builds](MakePath build) {
            builds.push_back(build);
        });

        while (!builds.empty()) {
            const MakePath sunk = sinkPastFilter(builds.pop_back_val(), builder);
            if (sunk) {
                builds.push_back(sunk);
            }
        }
    }
};

// A named path over one walk is the node the walk seeds from and its handle column, so
// what nodes(), relationships() and length() read off it is what the handles already
// answer: the seed then each hop's end, the edges, and the depth
struct WalkPath {
    MakePath _build;
    Value _seed;
    Value _path;
    bool _reversed {false};
};

bool matchWalkPath(PathElements elements, WalkPath& walk) {
    MakePath build = elements.getPath().getDefiningOp<MakePath>();
    if (!build) {
        return false;
    }

    const OperandRange entities = build.getEntities();
    if (entities.size() != 2) {
        return false;
    }

    const std::optional<ArrayAttr> reversedPaths = build.getReversedPaths();
    const bool reversed = reversedPaths && !reversedPaths->empty();

    const Value seed = reversed ? entities[1] : entities[0];
    const Value path = reversed ? entities[0] : entities[1];

    const auto elementOf = [](Value value) { return cast<ColumnType>(value.getType()).getType(); };
    if (!isa<storage::NodeIDType>(elementOf(seed)) || !isa<storage::PathRefType>(elementOf(path))) {
        return false;
    }

    walk = WalkPath {._build = build, ._seed = seed, ._path = path, ._reversed = reversed};

    return true;
}

Value walkPathElements(PathElements elements, const WalkPath& walk, mlir::OpBuilder& builder) {
    MLIRContext* const context = builder.getContext();
    const Location loc = elements.getLoc();

    switch (elements.getKind()) {
        case storage::PathElementsKind::Length: {
            const Type countType = ColumnType::get(context, IntegerType::get(context, 64, IntegerType::Unsigned));
            return builder.create<PathLength>(loc, countType, walk._path).getResult();
        }
        break;

        case storage::PathElementsKind::Relationships: {
            const Type listType = ColumnType::get(context, storage::ListType::get(context, storage::EdgeIDType::get(context)));
            return builder.create<ExpandPath>(loc,
                                              listType,
                                              walk._path,
                                              Value(),
                                              storage::PathExpansionKind::Edges,
                                              walk._reversed).getResult();
        }
        break;

        case storage::PathElementsKind::Nodes: {
            const Type listType = ColumnType::get(context, storage::ListType::get(context, storage::NodeIDType::get(context)));
            return builder.create<ExpandPath>(loc,
                                              listType,
                                              walk._path,
                                              walk._seed,
                                              storage::PathExpansionKind::Nodes,
                                              walk._reversed).getResult();
        }
        break;
    }

    return Value();
}

struct FusePathElements : public impl::FusePathElementsBase<FusePathElements> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<PathElements> reads;
        getOperation()->walk([&reads](PathElements elements) {
            reads.push_back(elements);
        });

        for (PathElements elements : reads) {
            WalkPath walk;
            if (!matchWalkPath(elements, walk)) {
                continue;
            }

            builder.setInsertionPoint(elements);
            const Value handles = walkPathElements(elements, walk, builder);

            Value result = elements.getResult();
            ToNullable widened = builder.create<ToNullable>(elements.getLoc(), result.getType(), handles);

            result.replaceAllUsesWith(widened.getResult());
            elements.erase();
            eraseIfUnused(walk._build);
        }
    }
};

// A path exploration whose rows are then cut down to those ending on the node a carried
// column already holds, or on the seed the walk left: the bound end spelled the long way,
// since the walk itself can head for that node and never build the rows the filter goes on
// to drop. An absent _endColumn is the seed, which no carried column names.
struct EndBoundExploration {
    ExplorePaths _exploration;
    EqOp _equality;
    std::optional<uint64_t> _endColumn;
};

bool matchEndBoundExploration(FilterOp filter, EndBoundExploration& bound) {
    EqOp equality = filter.getMask().getDefiningOp<EqOp>();
    if (!equality) {
        return false;
    }

    // One side is the end node column, the other the carried copy of the bound variable
    Value ends = equality.getLhs();
    Value carried = equality.getRhs();
    ExplorePaths exploration = ends.getDefiningOp<ExplorePaths>();
    if (!exploration || ends != exploration.getTgtids()) {
        std::swap(ends, carried);
        exploration = ends.getDefiningOp<ExplorePaths>();
    }

    const bool alreadyBound = exploration && (exploration.getEndColumn() || exploration.getEndsOnSeed() || exploration.getEndNodes());
    if (!exploration || ends != exploration.getTgtids() || alreadyBound) {
        return false;
    }

    const OpResult carriedResult = dyn_cast<OpResult>(carried);
    if (!carriedResult || carriedResult.getOwner() != exploration.getOperation()) {
        return false;
    }

    // A walk the pattern brings back to where it started compares its end against its own
    // seed, which is a result of the exploration like a carried column but not one of them
    const size_t resultIndex = carriedResult.getResultNumber();
    const bool endsOnSeed = carried == exploration.getSrcids();
    if (!endsOnSeed && resultIndex < pathFixedResultCount) {
        return false;
    }

    for (const Value column : filter.getColumnsToFilter()) {
        if (column.getDefiningOp() != exploration.getOperation()) {
            return false;
        }
    }

    for (const Value result : exploration.getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsThePair = user == filter.getOperation() || user == equality.getOperation();
            if (!readsThePair) {
                return false;
            }
        }
    }

    bound = EndBoundExploration {._exploration = exploration,
                                 ._equality = equality,
                                 ._endColumn = endsOnSeed
                                                 ? std::nullopt
                                                 : std::optional<uint64_t> {resultIndex - pathFixedResultCount}};

    return true;
}

IntegerAttr unsignedAttribute(uint64_t value, mlir::OpBuilder& builder) {
    const IntegerType type = IntegerType::get(builder.getContext(), 64, IntegerType::Unsigned);

    return IntegerAttr::get(type, value);
}

void fuseExploreEndNodes(FilterOp filter, const EndBoundExploration& bound, mlir::OpBuilder& builder) {
    ExplorePaths exploration = bound._exploration;
    EqOp equality = bound._equality;

    if (bound._endColumn) {
        exploration.setEndColumnAttr(unsignedAttribute(*bound._endColumn, builder));
    } else {
        exploration.setEndsOnSeed(true);
    }

    const Operation::operand_range columns = filter.getColumnsToFilter();
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    filter.erase();
    eraseIfUnused(equality);
}

struct FuseExploreEndNodes : public impl::FuseExploreEndNodesBase<FuseExploreEndNodes> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<EndBoundExploration>(getOperation(), matchEndBoundExploration, fuseExploreEndNodes, builder);
    }
};

// A path exploration whose end column is one factor of the cross product its seeds come
// from: every row carries the same targets, so the walk can head for them as a set and run
// once per seed instead of once per pair. That factor's rows are the set, so its ops move to
// the head of the function and the product goes on without the column.
struct FactorEndExploration {
    ExplorePaths _exploration;
    CrossProduct _product;
    size_t _endResult {0};
};

// The factor of a product yielding one of its results, and where in its yield
struct FactorColumn {
    Region* _factor {nullptr};
    Region* _other {nullptr};
    size_t _position {0};
    bool _left {false};
};

void locateFactorColumn(CrossProduct product, size_t resultIndex, FactorColumn& column) {
    Region& leftFactor = product.getLeftFactor();
    Region& rightFactor = product.getRightFactor();
    const size_t leftCount = factorYieldColumns(leftFactor).size();

    column._left = resultIndex < leftCount;
    column._factor = column._left ? &leftFactor : &rightFactor;
    column._other = column._left ? &rightFactor : &leftFactor;
    column._position = column._left ? resultIndex : resultIndex - leftCount;
}

// Whether the result can be taken out of the product as a set: its factor yields it alone,
// or yields it off a product of its own that nothing but the yield reads, and so on down
bool canHoistFactorColumn(CrossProduct product, size_t resultIndex) {
    CrossProduct current = product;
    size_t currentIndex = resultIndex;
    while (true) {
        FactorColumn column;
        locateFactorColumn(current, currentIndex, column);

        const Operation::operand_range yielded = factorYieldColumns(*column._factor);
        if (yielded.size() == 1) {
            return true;
        }

        const Value value = yielded[column._position];
        CrossProduct inner = value.getDefiningOp<CrossProduct>();
        if (!inner || inner->getParentRegion() != column._factor) {
            return false;
        }

        Operation* const yield = column._factor->front().getTerminator();
        for (const Value result : inner.getResults()) {
            for (Operation* const user : result.getUsers()) {
                if (user != yield) {
                    return false;
                }
            }
        }

        current = inner;
        currentIndex = cast<OpResult>(value).getResultNumber();
    }
}

bool matchFactorEndExploration(ExplorePaths exploration, FactorEndExploration& match) {
    const std::optional<uint64_t> endColumn = exploration.getEndColumn();
    if (!endColumn || exploration.getEndNodes()) {
        return false;
    }

    const Value input = exploration.getInputNodes();
    const Value end = exploration.getColumnsToFilter()[*endColumn];
    CrossProduct product = input.getDefiningOp<CrossProduct>();
    if (!product || end.getDefiningOp() != product.getOperation()) {
        return false;
    }

    // A target drawn from the seed's own factor differs row by row
    const size_t inputResult = cast<OpResult>(input).getResultNumber();
    const size_t endResult = cast<OpResult>(end).getResultNumber();
    FactorColumn seed;
    FactorColumn target;
    locateFactorColumn(product, inputResult, seed);
    locateFactorColumn(product, endResult, target);
    if (seed._factor == target._factor) {
        return false;
    }

    for (const Value result : product.getResults()) {
        for (Operation* const user : result.getUsers()) {
            if (user != exploration.getOperation()) {
                return false;
            }
        }
    }

    if (!canHoistFactorColumn(product, endResult)) {
        return false;
    }

    match = FactorEndExploration {._exploration = exploration, ._product = product, ._endResult = endResult};

    return true;
}

void moveFactorBody(Region& factor, Block* target, Block::iterator position) {
    Block& block = factor.front();
    target->getOperations().splice(position, block.getOperations(), block.begin(), Block::iterator(block.getTerminator()));
}

// One product on the way down to the factor yielding a hoisted result alone
struct FactorColumnLevel {
    CrossProduct _product {nullptr};
    size_t _resultIndex {0};
    FactorColumn _column;
};

Value collapseFactorColumn(const FactorColumnLevel& level, Block* head) {
    CrossProduct product = level._product;
    const FactorColumn& column = level._column;

    const llvm::SmallVector<Value> yielded(factorYieldColumns(*column._factor));
    const llvm::SmallVector<Value> otherYielded(factorYieldColumns(*column._other));

    moveFactorBody(*column._factor, head, head->begin());
    moveFactorBody(*column._other, product->getBlock(), Block::iterator(product));

    const size_t firstOtherResult = column._left ? 1 : 0;
    for (size_t index = 0; index < otherYielded.size(); index++) {
        product.getResult(firstOtherResult + index).replaceAllUsesWith(otherYielded[index]);
    }

    const Value hoisted = yielded.front();
    product.getResult(level._resultIndex).replaceAllUsesWith(hoisted);
    product.erase();

    return hoisted;
}

void narrowFactorColumn(const FactorColumnLevel& level, Value hoisted, mlir::OpBuilder& builder) {
    CrossProduct product = level._product;
    const FactorColumn& column = level._column;
    const size_t resultIndex = level._resultIndex;

    Yield yield = cast<Yield>(column._factor->front().getTerminator());
    yield.getColumnsMutable().erase(static_cast<unsigned>(column._position));

    llvm::SmallVector<Type> narrowedTypes;
    for (size_t index = 0; index < product.getNumResults(); index++) {
        if (index != resultIndex) {
            narrowedTypes.push_back(product.getResult(index).getType());
        }
    }

    builder.setInsertionPoint(product);
    CrossProduct narrowed = builder.create<CrossProduct>(product.getLoc(), narrowedTypes);
    narrowed.getLeftFactor().takeBody(product.getLeftFactor());
    narrowed.getRightFactor().takeBody(product.getRightFactor());

    size_t kept = 0;
    for (size_t index = 0; index < product.getNumResults(); index++) {
        if (index == resultIndex) {
            product.getResult(index).replaceAllUsesWith(hoisted);
        } else {
            product.getResult(index).replaceAllUsesWith(narrowed.getResult(kept));
            kept++;
        }
    }

    product.erase();
}

// Takes the result out of the product as a value at the head of the function and leaves the
// product without it: collapsed to its other factor when the result was all its factor
// yielded, rebuilt one column narrower otherwise
Value hoistFactorColumn(CrossProduct product, size_t resultIndex, Block* head, mlir::OpBuilder& builder) {
    llvm::SmallVector<FactorColumnLevel> levels;
    CrossProduct current = product;
    size_t currentIndex = resultIndex;
    while (true) {
        FactorColumnLevel level {._product = current, ._resultIndex = currentIndex};
        locateFactorColumn(current, currentIndex, level._column);
        levels.push_back(level);

        const Operation::operand_range yielded = factorYieldColumns(*level._column._factor);
        if (yielded.size() == 1) {
            break;
        }

        const Value value = yielded[level._column._position];
        current = value.getDefiningOp<CrossProduct>();
        currentIndex = cast<OpResult>(value).getResultNumber();
    }

    const Value hoisted = collapseFactorColumn(levels.back(), head);
    for (const FactorColumnLevel& level : llvm::reverse(llvm::ArrayRef(levels).drop_back())) {
        narrowFactorColumn(level, hoisted, builder);
    }

    return hoisted;
}

void fuseExploreEndFactor(const FactorEndExploration& match, mlir::OpBuilder& builder) {
    ExplorePaths exploration = match._exploration;
    CrossProduct product = match._product;
    const size_t endColumn = *exploration.getEndColumn();
    const Value endResult = product.getResult(match._endResult);

    const Operation::operand_range carried = exploration.getColumnsToFilter();
    const mlir::ResultRange carriedResults = exploration.getFilteredColumns();

    llvm::SmallVector<Value> keptColumns;
    llvm::SmallVector<Type> resultTypes {exploration.getSrcids().getType(), exploration.getTgtids().getType(), exploration.getPaths().getType()};
    for (size_t index = 0; index < carried.size(); index++) {
        if (index == endColumn) {
            continue;
        }

        keptColumns.push_back(carried[index]);
        resultTypes.push_back(carriedResults[index].getType());
    }

    // The set is the product's own result until the hoist puts the factor's value in its place
    builder.setInsertionPoint(exploration);
    ExplorePaths set = builder.create<ExplorePaths>(exploration.getLoc(),
                                                    resultTypes,
                                                    exploration.getInputNodes(),
                                                    keptColumns,
                                                    endResult,
                                                    exploration.getHopImports(),
                                                    exploration.getDirection(),
                                                    exploration.getMinHops(),
                                                    exploration.getMaxHopsAttr(),
                                                    exploration.getEdgeTypesAttr(),
                                                    exploration.getEndLabelsAttr(),
                                                    exploration.getHopLabelsAttr(),
                                                    IntegerAttr(),
                                                    false,
                                                    exploration.getDistinct());
    set.getHop().takeBody(exploration.getHop());

    exploration.getSrcids().replaceAllUsesWith(set.getSrcids());
    exploration.getTgtids().replaceAllUsesWith(set.getTgtids());
    exploration.getPaths().replaceAllUsesWith(set.getPaths());

    const mlir::ResultRange setCarried = set.getFilteredColumns();
    size_t kept = 0;
    for (size_t index = 0; index < carried.size(); index++) {
        if (index == endColumn) {
            carriedResults[index].replaceAllUsesWith(set.getTgtids());
        } else {
            carriedResults[index].replaceAllUsesWith(setCarried[kept]);
            kept++;
        }
    }

    exploration.erase();

    mlir::func::FuncOp function = set->getParentOfType<mlir::func::FuncOp>();
    hoistFactorColumn(product, match._endResult, &function.getBody().front(), builder);
}

struct FuseExploreEndFactor : public impl::FuseExploreEndFactorBase<FuseExploreEndFactor> {
    void runOnOperation() override {
        llvm::SmallVector<ExplorePaths> explorations;
        getOperation()->walk([&](ExplorePaths exploration) {
            explorations.push_back(exploration);
        });

        mlir::OpBuilder builder(&getContext());
        for (ExplorePaths exploration : explorations) {
            FactorEndExploration match;
            if (matchFactorEndExploration(exploration, match)) {
                fuseExploreEndFactor(match, builder);
            }
        }
    }
};

// A path exploration whose rows are then cut down to those ending on a node whose property
// holds a literal: the end set spelled the long way, since the nodes holding the value can
// be scanned before the walk and handed to it as the set to head for, and the prefixes that
// cannot reach one are then not walked. The end's labels go into that scan.
struct EndSetExploration {
    ExplorePaths _exploration;
    GetNodeProperties _read;
    EqOp _equality;
    ConstantOp _constant;
};

bool matchEndSetExploration(FilterOp filter, EndSetExploration& endSet) {
    EqOp equality = filter.getMask().getDefiningOp<EqOp>();
    if (!equality || !equality.getResult().hasOneUse()) {
        return false;
    }

    // One side reads the property of the end nodes, the other is the literal
    GetNodeProperties read = equality.getLhs().getDefiningOp<GetNodeProperties>();
    Value literalSide = equality.getRhs();
    if (!read) {
        read = equality.getRhs().getDefiningOp<GetNodeProperties>();
        literalSide = equality.getLhs();
    }

    // A read of the write buffer sees nodes no scan of the graph does
    const bool readsCommittedEnds = read && !read.getPending() && !read.getAllPending() && read.getResult().hasOneUse();
    if (!readsCommittedEnds) {
        return false;
    }

    const Value ends = read.getInputNodes();
    ExplorePaths exploration = ends.getDefiningOp<ExplorePaths>();
    const bool alreadyBound = exploration && (exploration.getEndColumn() || exploration.getEndsOnSeed() || exploration.getEndNodes());
    if (!exploration || ends != exploration.getTgtids() || alreadyBound) {
        return false;
    }

    ConstantOp constant = literalSide.getDefiningOp<ConstantOp>();
    if (!constant) {
        return false;
    }

    const TypedAttr literal = dyn_cast<TypedAttr>(constant.getValue());
    if (!literal || !storage::isPropertyScanLiteral(literal)) {
        return false;
    }

    for (const Value column : filter.getColumnsToFilter()) {
        if (column.getDefiningOp() != exploration.getOperation()) {
            return false;
        }
    }

    for (const Value result : exploration.getResults()) {
        for (Operation* const user : result.getUsers()) {
            const bool readsThePair = user == filter.getOperation() || user == read.getOperation();
            if (!readsThePair) {
                return false;
            }
        }
    }

    endSet = EndSetExploration {._exploration = exploration, ._read = read, ._equality = equality, ._constant = constant};

    return true;
}

void fuseExploreEndSet(FilterOp filter, const EndSetExploration& endSet, mlir::OpBuilder& builder) {
    ExplorePaths exploration = endSet._exploration;
    GetNodeProperties read = endSet._read;
    EqOp equality = endSet._equality;
    ConstantOp constant = endSet._constant;

    // Lowering fills the set in a loop of its own that has to close before the walk's opens,
    // so the scan stands at the head of the function, ahead of whatever feeds the seeds
    mlir::func::FuncOp function = exploration->getParentOfType<mlir::func::FuncOp>();
    builder.setInsertionPointToStart(&function.getBody().front());

    ScanNodesByPropertyValue set = builder.create<ScanNodesByPropertyValue>(exploration.getLoc(),
                                                                            exploration.getTgtids().getType(),
                                                                            read.getPropertyAttr(),
                                                                            cast<TypedAttr>(constant.getValue()),
                                                                            exploration.getEndLabelsAttr());

    exploration.getEndNodesMutable().assign(set.getResult());
    exploration.removeEndLabelsAttr();

    const Operation::operand_range columns = filter.getColumnsToFilter();
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    filter.erase();
    eraseIfUnused(equality);
    eraseIfUnused(read);
    eraseIfUnused(constant);
}

struct FuseExploreEndSet : public impl::FuseExploreEndSetBase<FuseExploreEndSet> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<EndSetExploration>(getOperation(), matchEndSetExploration, fuseExploreEndSet, builder);
    }
};

// A filter keeping the walks whose every hop passes a test, spelled as all() or none() over
// the relationships, the nodes or a group variable of the path: the walk can run the test on
// each hop as it takes it, and never expand a hop that fails.
struct ExploreListPredicate {
    FilterOp _filter;
    ExplorePaths _exploration;
    ListPredicate _predicate;
    ExpandPath _expansion;
    llvm::SmallVector<Value> _imports;
};

Value climbFilters(Value column) {
    while (FilterOp filter = column.getDefiningOp<FilterOp>()) {
        column = filter.getColumnsToFilter()[cast<OpResult>(column).getResultNumber()];
    }

    return column;
}

bool keepsTheWalkedRows(Operation& op) {
    return isa<FilterOp, ExpandPath, ListPredicate>(op) || computesPerRow(&op);
}

// The operand of the exploration a column of its rows repeats for every path of a seed, or
// null for a column the walk binds
Value seedColumnOf(ExplorePaths exploration, Value column) {
    const Value climbed = climbFilters(column);
    if (climbed.getDefiningOp() != exploration.getOperation()) {
        return {};
    }

    const size_t resultIndex = cast<OpResult>(climbed).getResultNumber();
    constexpr size_t TARGET_RESULT_INDEX = 1;
    const bool holdsTheSeed = resultIndex == 0 || (resultIndex == TARGET_RESULT_INDEX && exploration.getEndsOnSeed());

    if (holdsTheSeed) {
        return exploration.getInputNodes();
    } else if (resultIndex >= pathFixedResultCount) {
        return exploration.getColumnsToFilter()[resultIndex - pathFixedResultCount];
    } else {
        return {};
    }
}

// Whether the body reads only the element, constants and carried columns holding one value per
// seed. Each carried argument the body reads gets the exploration operand to import, the rest null.
bool matchHopTest(ListPredicate predicate, ExplorePaths exploration, llvm::SmallVectorImpl<Value>& imports) {
    Block& body = predicate.getBody().front();
    const BlockArgument tag = body.getArgument(1);
    ComprehensionYield yield = cast<ComprehensionYield>(body.getTerminator());

    const auto readsOnlyTheHop = [&body, tag](Value operand) {
        if (const BlockArgument argument = dyn_cast<BlockArgument>(operand)) {
            return argument.getOwner() == &body && argument != tag;
        }

        return operand.getDefiningOp()->getBlock() == &body || ::db::yieldsConstantColumn(operand);
    };

    for (Operation& op : body.without_terminator()) {
        const bool holdsARegion = op.getNumRegions() != 0;
        const bool readsOutsideTheHop = !llvm::all_of(op.getOperands(), readsOnlyTheHop);
        if (holdsARegion || readsOutsideTheHop) {
            return false;
        }
    }

    const bool cutsElements = yield.getRowTags() != tag;
    const bool yieldsOutsideTheHop = !readsOnlyTheHop(yield.getValue());
    if (cutsElements || yieldsOutsideTheHop) {
        return false;
    }

    const Operation::operand_range carried = predicate.getColumnsToFilter();
    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        if (body.getArgument(carriedIndex + 2).use_empty()) {
            imports.push_back({});
            continue;
        }

        const Value seedColumn = seedColumnOf(exploration, carried[carriedIndex]);
        if (!seedColumn) {
            return false;
        }

        imports.push_back(seedColumn);
    }

    return true;
}

bool matchExploreListPredicate(FilterOp filter, ExploreListPredicate& match) {
    ListPredicate predicate = filter.getMask().getDefiningOp<ListPredicate>();
    if (!predicate || !predicate.getResult().hasOneUse()) {
        return false;
    }

    const storage::ListPredicateKind kind = predicate.getKind();
    const bool holdsForEveryElement = kind == storage::ListPredicateKind::All || kind == storage::ListPredicateKind::None;
    if (!holdsForEveryElement) {
        return false;
    }

    Value source = predicate.getSource();
    if (ToNullable widened = source.getDefiningOp<ToNullable>()) {
        source = widened.getOperand();
    }

    ExpandPath expansion = source.getDefiningOp<ExpandPath>();
    if (!expansion) {
        return false;
    }

    const Value paths = climbFilters(expansion.getPaths());
    ExplorePaths exploration = paths.getDefiningOp<ExplorePaths>();
    const bool walksThePaths = exploration && paths == exploration.getPaths();
    if (!walksThePaths) {
        return false;
    }

    const bool reachesTheFilter = exploration->getBlock() == filter->getBlock()
                               && rowsReachTheFilter(exploration, filter, keepsTheWalkedRows);
    if (!reachesTheFilter) {
        return false;
    }

    llvm::SmallVector<Value> imports;
    if (!matchHopTest(predicate, exploration, imports)) {
        return false;
    }

    const bool importsAColumn = llvm::any_of(imports, [](Value import) {
        return static_cast<bool>(import);
    });

    if (importsAColumn && exploration.getDistinct()) {
        return false;
    }

    match = ExploreListPredicate {._filter = filter,
                                  ._exploration = exploration,
                                  ._predicate = predicate,
                                  ._expansion = expansion,
                                  ._imports = imports};

    return true;
}

unsigned hopArgumentOf(storage::PathExpansionKind expansionKind) {
    constexpr unsigned HOP_SOURCE_ARGUMENT = 0;
    constexpr unsigned HOP_EDGE_ARGUMENT = 1;
    constexpr unsigned HOP_END_ARGUMENT = 2;

    switch (expansionKind) {
        case storage::PathExpansionKind::Sources:
            return HOP_SOURCE_ARGUMENT;
        break;
        case storage::PathExpansionKind::Edges:
            return HOP_EDGE_ARGUMENT;
        break;
        case storage::PathExpansionKind::Ends:
        case storage::PathExpansionKind::Nodes:
            return HOP_END_ARGUMENT;
        break;
    }

    llvm_unreachable("Unknown path expansion kind");
}

// Clones the predicate's body at the rewriter's insertion point over the element column and
// the columns its carried arguments stand for, and returns the test it yields: negated for
// none(), which holds where all() of the negation does
Value cloneElementTest(ListPredicate predicate, Value element, llvm::ArrayRef<Value> carried, mlir::RewriterBase& rewriter) {
    Block& body = predicate.getBody().front();

    mlir::IRMapping mapping;
    mapping.map(body.getArgument(0), element);
    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        if (carried[carriedIndex]) {
            mapping.map(body.getArgument(carriedIndex + 2), carried[carriedIndex]);
        }
    }

    for (Operation& op : body.without_terminator()) {
        rewriter.clone(op, mapping);
    }

    ComprehensionYield yield = cast<ComprehensionYield>(body.getTerminator());
    const Value test = mapping.lookupOrDefault(yield.getValue());

    if (predicate.getKind() == storage::ListPredicateKind::All) {
        return test;
    }

    return rewriter.create<NotOp>(predicate.getLoc(), boolColumnType(rewriter.getContext()), test).getResult();
}

Block* hopBlockOf(ExplorePaths exploration, mlir::RewriterBase& rewriter) {
    Region& hop = exploration.getHop();
    if (!hop.empty()) {
        return &hop.front();
    }

    mlir::MLIRContext* context = rewriter.getContext();
    const Type nodeType = ColumnType::get(context, storage::NodeIDType::get(context));
    const Type edgeType = ColumnType::get(context, storage::EdgeIDType::get(context));
    const llvm::SmallVector<Type> argumentTypes {nodeType, edgeType, nodeType};
    const llvm::SmallVector<Location> argumentLocations(argumentTypes.size(), exploration.getLoc());

    const mlir::OpBuilder::InsertionGuard guard(rewriter);

    return rewriter.createBlock(&hop, hop.end(), argumentTypes, argumentLocations);
}

Value hopImportArgument(ExplorePaths exploration, Block* hopBlock, Value import, mlir::RewriterBase& rewriter) {
    constexpr size_t HOP_ARGUMENT_COUNT = 3;

    const Operation::operand_range imports = exploration.getHopImports();
    for (size_t importIndex = 0; importIndex < imports.size(); importIndex++) {
        if (imports[importIndex] == import) {
            return hopBlock->getArgument(HOP_ARGUMENT_COUNT + importIndex);
        }
    }

    rewriter.modifyOpInPlace(exploration, [&exploration, import]() {
        exploration.getHopImportsMutable().append(import);
    });

    return hopBlock->addArgument(import.getType(), exploration.getLoc());
}

void fuseExploreListPredicate(ExploreListPredicate& match, mlir::RewriterBase& rewriter) {
    FilterOp filter = match._filter;
    ExplorePaths exploration = match._exploration;
    ListPredicate predicate = match._predicate;
    ExpandPath expansion = match._expansion;
    const Location loc = predicate.getLoc();

    Block* hopBlock = hopBlockOf(exploration, rewriter);

    llvm::SmallVector<Value> hopCarried;
    for (const Value import : match._imports) {
        hopCarried.push_back(import ? hopImportArgument(exploration, hopBlock, import, rewriter) : Value {});
    }

    const storage::PathExpansionKind expansionKind = expansion.getKind();
    const Value hopElement = hopBlock->getArgument(hopArgumentOf(expansionKind));

    Yield hopYield = hopBlock->empty() ? Yield {} : dyn_cast<Yield>(hopBlock->getTerminator());
    if (hopYield) {
        rewriter.setInsertionPoint(hopYield);
    } else {
        rewriter.setInsertionPointToEnd(hopBlock);
    }

    const Value hopTest = cloneElementTest(predicate, hopElement, hopCarried, rewriter);

    if (hopYield) {
        const Type boolType = boolColumnType(rewriter.getContext());
        const Value joined = rewriter.create<AndOp>(loc, boolType, hopYield.getColumns().front(), hopTest).getResult();

        rewriter.modifyOpInPlace(hopYield, [&hopYield, joined]() {
            hopYield->setOperand(0, joined);
        });
    } else {
        rewriter.create<Yield>(loc, ValueRange {hopTest});
    }

    if (expansionKind != storage::PathExpansionKind::Nodes) {
        bypassFilter(filter);
        rewriter.eraseOp(filter);
    } else {
        // nodes() holds the seed, which no hop ends on, so the seed is still tested row by row
        const Operation::operand_range predicateCarried = predicate.getColumnsToFilter();
        const llvm::SmallVector<Value> rowCarried(predicateCarried.begin(), predicateCarried.end());

        rewriter.setInsertionPoint(predicate);
        const Value seedTest = cloneElementTest(predicate, expansion.getSrcids(), rowCarried, rewriter);

        rewriter.modifyOpInPlace(filter, [&filter, seedTest]() {
            filter->setOperand(0, seedTest);
        });
    }

    Operation* const widened = predicate.getSource().getDefiningOp<ToNullable>();
    rewriter.eraseOp(predicate);

    if (widened && widened->use_empty()) {
        rewriter.eraseOp(widened);
    }

    if (expansion->use_empty()) {
        rewriter.eraseOp(expansion);
    }
}

struct FuseExploreListPredicate : public impl::FuseExploreListPredicateBase<FuseExploreListPredicate> {
    void runOnOperation() override {
        runFilterWorklist<ExploreListPredicate>(getOperation(), matchExploreListPredicate, fuseExploreListPredicate);
    }
};

// A row-wise op: one output row per input row, computed from that row alone, so a relation
// deduplicated before it yields the same set after it.
bool isRowWiseOp(Operation* op) {
    return isMaskComputeOp(op)
        || isa<GetNodeLabelSet, CheckLabelConstraint, CheckEdgeTypeConstraint,
               Labels, EdgeType, ToInteger, ToFloat, ToBoolean,
               CosineSimilarity, EuclideanDistance>(op);
}

bool aggregatesDistinctly(llvm::ArrayRef<int64_t> kinds) {
    for (const int64_t kind : kinds) {
        switch (static_cast<storage::GroupAggregateKind>(kind)) {
            case storage::GroupAggregateKind::Min:
            case storage::GroupAggregateKind::Max:
            case storage::GroupAggregateKind::CountDistinct:
            case storage::GroupAggregateKind::SumDistinct:
            case storage::GroupAggregateKind::AvgDistinct:
            break;

            case storage::GroupAggregateKind::Count:
            case storage::GroupAggregateKind::Sum:
            case storage::GroupAggregateKind::Avg:
            case storage::GroupAggregateKind::CountRows:
                return false;
            break;
        }
    }

    return true;
}

size_t collectValueCount(Collect collect) {
    const size_t aggregateCount = collect.getKinds().value_or(llvm::ArrayRef<int64_t> {}).size();

    return collect.getColumns().size() - collect.getKeyCount() - aggregateCount;
}

// Every list a collect builds dedupes its values, and every aggregate beside them ignores a
// repeated row
bool collectsDistinctly(Collect collect) {
    const llvm::ArrayRef<int64_t> kinds = collect.getKinds().value_or(llvm::ArrayRef<int64_t> {});
    const llvm::ArrayRef<int64_t> distinctValues = collect.getDistinctValues().value_or(llvm::ArrayRef<int64_t> {});
    const llvm::SmallDenseSet<int64_t, 4> dedupedValues(distinctValues.begin(), distinctValues.end());

    return dedupedValues.size() == collectValueCount(collect) && aggregatesDistinctly(kinds);
}

bool readsRowsAsASet(Operation* op, llvm::SmallVectorImpl<Operation*>& passedOn) {
    if (isa<RemoveDuplicates>(op)) {
        return true;
    } else if (Count count = dyn_cast<Count>(op)) {
        return count.getDistinct();
    } else if (GroupAggregate groupAggregate = dyn_cast<GroupAggregate>(op)) {
        return aggregatesDistinctly(groupAggregate.getKinds());
    } else if (Collect collect = dyn_cast<Collect>(op)) {
        return collectsDistinctly(collect);
    } else if (isRowWiseOp(op) || isa<FilterOp, ExplorePaths>(op) || isEdgeHop(op)) {
        passedOn.push_back(op);
        return true;
    }

    return false;
}

// Whether every row the op ever emits is only ever read as a member of a set: a dedup or a
// distinct aggregate settles it, a row-wise op, a filter or a further hop passes the question
// on to its own users, and anything counting, cutting or outputting rows refuses.
bool usersReadRowsAsASet(Operation* op) {
    llvm::SmallPtrSet<Operation*, 16> visited;
    llvm::SmallVector<Operation*> passedOn {op};
    while (!passedOn.empty()) {
        Operation* const producer = passedOn.pop_back_val();

        for (Operation* const user : producer->getUsers()) {
            const bool firstVisit = visited.insert(user).second;
            if (firstVisit && !readsRowsAsASet(user, passedOn)) {
                return false;
            }
        }
    }

    return true;
}

bool matchDistinctEnds(ExplorePaths exploration) {
    if (exploration.getDistinct() || !exploration.getPaths().use_empty()) {
        return false;
    }

    // The search expands a batch of seeds one level at a time, so it has no one row to read
    // a hop import at: a walk whose predicate reads outside the hop stays a walk
    if (!exploration.getHopImports().empty()) {
        return false;
    }

    return usersReadRowsAsASet(exploration.getOperation());
}

struct FuseExploreDistinctEnds : public impl::FuseExploreDistinctEndsBase<FuseExploreDistinctEnds> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        getOperation()->walk([&](ExplorePaths exploration) {
            if (matchDistinctEnds(exploration)) {
                exploration.setDistinctAttr(builder.getUnitAttr());
            }
        });
    }
};

// A count reads no value off a path, and a path is never null, so count(p) tallies the
// rows its build ran over. The tally moves onto the node the path opens on, which the
// build already read: row aligned with the path and never null in its own right.
bool matchBuiltPath(Count count, Value& rows) {
    if (count.getDistinct() || count.getRows()) {
        return false;
    }

    MakePath build = dyn_cast_or_null<MakePath>(count.getInput().getDefiningOp());
    if (!build) {
        return false;
    }

    rows = build.getEntities().front();

    return true;
}

struct CountPathRows : public impl::CountPathRowsBase<CountPathRows> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<Count> counts;
        getOperation()->walk([&counts](Count count) {
            counts.push_back(count);
        });

        for (Count count : counts) {
            Value rows;
            if (!matchBuiltPath(count, rows)) {
                continue;
            }

            Operation* build = count.getInput().getDefiningOp();

            count->setOperand(0, rows);
            count.setRowsAttr(builder.getUnitAttr());

            if (build->use_empty()) {
                build->erase();
            }
        }
    }
};

// Where an op's carry set sits: the carried operands start at _operandOffset and each comes
// back as the result at the same position from _resultOffset. _trailingOperandCount is what
// the op takes after its carry set, which a trim keeps as it found it.
struct CarrySetLayout {
    size_t _operandOffset {0};
    size_t _resultOffset {0};
    size_t _trailingOperandCount {0};
};

bool matchCarrySetLayout(Operation* op, CarrySetLayout& layout) {
    if (isEdgeHop(op)) {
        layout = CarrySetLayout {._operandOffset = 1, ._resultOffset = hopFixedResultCount};
        return true;
    } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op)) {
        const size_t endNodeCount = exploration.getEndNodes() ? 1u : 0u;
        layout = CarrySetLayout {._operandOffset = 1,
                                 ._resultOffset = pathFixedResultCount,
                                 ._trailingOperandCount = endNodeCount + exploration.getHopImports().size()};
        return true;
    } else if (isa<FilterOp>(op)) {
        layout = CarrySetLayout {._operandOffset = 1, ._resultOffset = 0};
        return true;
    } else if (isa<Unwind, FetchNodes>(op)) {
        layout = CarrySetLayout {._operandOffset = 1, ._resultOffset = 1};
        return true;
    } else if (isa<Limit, Skip, Sort, GroupAggregate, Collect>(op)) {
        layout = CarrySetLayout {._operandOffset = 0, ._resultOffset = 0};
        return true;
    } else if (CallProcedure call = dyn_cast<CallProcedure>(op)) {
        const size_t inputCount = call.getInputs().size();
        const size_t yieldCount = call.getYields().size();
        layout = CarrySetLayout {._operandOffset = inputCount, ._resultOffset = yieldCount};
        return true;
    }

    return false;
}

bool trimsColumns(Operation* op) {
    CarrySetLayout layout;

    return matchCarrySetLayout(op, layout) || isa<CrossProduct, HashJoin>(op);
}

size_t carriedCount(Operation* op, const CarrySetLayout& layout) {
    return op->getNumOperands() - layout._operandOffset - layout._trailingOperandCount;
}

void keepOneOf(llvm::SmallBitVector& keep, size_t begin, size_t end) {
    if (begin >= end) {
        return;
    }

    for (size_t index = begin; index < end; index++) {
        if (keep[index]) {
            return;
        }
    }

    keep.set(begin);
}

// A sort orders by its keys and a grouping op groups by them whether or not anything reads
// them; a group aggregate still has to reduce something and a collect to gather something.
void keepRequiredColumns(Operation* op, llvm::SmallBitVector& keep) {
    if (Sort sort = dyn_cast<Sort>(op)) {
        for (const int64_t keyColumn : sort.getKeyColumns()) {
            keep.set(static_cast<unsigned>(keyColumn));
        }
    } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op)) {
        if (const std::optional<uint64_t> endColumn = exploration.getEndColumn()) {
            keep.set(static_cast<unsigned>(*endColumn));
        }
    } else if (GroupAggregate groupAggregate = dyn_cast<GroupAggregate>(op)) {
        const size_t keyCount = groupAggregate.getKeyCount();
        keep.set(0, keyCount);
        keepOneOf(keep, keyCount, keep.size());
    } else if (Collect collect = dyn_cast<Collect>(op)) {
        const size_t keyCount = collect.getKeyCount();
        keep.set(0, keyCount);
        keepOneOf(keep, keyCount, keyCount + collectValueCount(collect));
    }
}

// An op standing for a relation is sized by a row-carrying column of it during lowering, so
// one has to survive whenever the op had one.
void keepRowCarryingColumn(ValueRange columns, size_t firstIndex, llvm::SmallBitVector& keep) {
    std::optional<size_t> firstRowCarrying;
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (::db::yieldsConstantColumn(columns[columnIndex])) {
            continue;
        }

        if (keep[firstIndex + columnIndex]) {
            return;
        }

        if (!firstRowCarrying) {
            firstRowCarrying = columnIndex;
        }
    }

    if (firstRowCarrying) {
        keep.set(firstIndex + *firstRowCarrying);
    } else if (!columns.empty()) {
        keepOneOf(keep, firstIndex, firstIndex + columns.size());
    }
}

void selectKeptCarriedColumns(Operation* op, const CarrySetLayout& layout, llvm::SmallVectorImpl<size_t>& kept) {
    const size_t count = carriedCount(op, layout);

    llvm::SmallBitVector keep(count);
    for (size_t carriedIndex = 0; carriedIndex < count; carriedIndex++) {
        if (!op->getResult(layout._resultOffset + carriedIndex).use_empty()) {
            keep.set(carriedIndex);
        }
    }

    keepRequiredColumns(op, keep);

    const bool standsForItsRows = layout._resultOffset == 0;
    if (standsForItsRows) {
        keepRowCarryingColumn(op->getOperands().drop_front(layout._operandOffset), 0, keep);
    }

    for (size_t carriedIndex = 0; carriedIndex < count; carriedIndex++) {
        if (keep[carriedIndex]) {
            kept.push_back(carriedIndex);
        }
    }
}

void setOrEraseIndices(OperationState& state, StringAttr name, llvm::ArrayRef<int64_t> indices, mlir::OpBuilder& builder) {
    if (indices.empty()) {
        state.attributes.erase(name);
    } else {
        state.attributes.set(name, builder.getDenseI64ArrayAttr(indices));
    }
}

void renumberSortKeys(Sort sort, llvm::ArrayRef<size_t> kept, OperationState& state, mlir::OpBuilder& builder) {
    llvm::SmallVector<int64_t> keyColumns;
    for (const int64_t keyColumn : sort.getKeyColumns()) {
        const auto keptIt = llvm::find(kept, static_cast<size_t>(keyColumn));
        bioassert(keptIt != kept.end(), "Sort key column {} is not in the trimmed carry set", keyColumn);

        keyColumns.push_back(static_cast<int64_t>(keptIt - kept.begin()));
    }

    state.attributes.set(sort.getKeyColumnsAttrName(), builder.getDenseI64ArrayAttr(keyColumns));
}

void trimGroupAggregateKinds(GroupAggregate groupAggregate, llvm::ArrayRef<size_t> kept, OperationState& state, mlir::OpBuilder& builder) {
    const size_t keyCount = groupAggregate.getKeyCount();
    const llvm::ArrayRef<int64_t> kinds = groupAggregate.getKinds();

    llvm::SmallVector<int64_t> keptKinds;
    for (const size_t columnIndex : kept) {
        if (columnIndex >= keyCount) {
            keptKinds.push_back(kinds[columnIndex - keyCount]);
        }
    }

    state.attributes.set(groupAggregate.getKindsAttrName(), builder.getDenseI64ArrayAttr(keptKinds));
}

void trimCollectAttributes(Collect collect, llvm::ArrayRef<size_t> kept, OperationState& state, mlir::OpBuilder& builder) {
    const size_t keyCount = collect.getKeyCount();
    const size_t valueEnd = keyCount + collectValueCount(collect);
    const llvm::ArrayRef<int64_t> kinds = collect.getKinds().value_or(llvm::ArrayRef<int64_t> {});

    llvm::SmallVector<int64_t> keptKinds;
    llvm::SmallVector<int64_t> keptValues;
    for (const size_t columnIndex : kept) {
        if (columnIndex >= valueEnd) {
            keptKinds.push_back(kinds[columnIndex - valueEnd]);
        } else if (columnIndex >= keyCount) {
            keptValues.push_back(static_cast<int64_t>(columnIndex - keyCount));
        }
    }

    llvm::SmallVector<int64_t> distinctValues;
    for (const int64_t valueIndex : collect.getDistinctValues().value_or(llvm::ArrayRef<int64_t> {})) {
        const auto keptIt = llvm::find(keptValues, valueIndex);
        if (keptIt != keptValues.end()) {
            distinctValues.push_back(static_cast<int64_t>(keptIt - keptValues.begin()));
        }
    }

    setOrEraseIndices(state, collect.getKindsAttrName(), keptKinds, builder);
    setOrEraseIndices(state, collect.getDistinctValuesAttrName(), distinctValues, builder);
}

void renumberEndColumn(ExplorePaths exploration, llvm::ArrayRef<size_t> kept, OperationState& state, mlir::OpBuilder& builder) {
    const std::optional<uint64_t> endColumn = exploration.getEndColumn();
    if (!endColumn) {
        return;
    }

    const auto keptIt = llvm::find(kept, static_cast<size_t>(*endColumn));
    bioassert(keptIt != kept.end(), "End column {} is not in the trimmed carry set", *endColumn);

    state.attributes.set(exploration.getEndColumnAttrName(), unsignedAttribute(static_cast<uint64_t>(keptIt - kept.begin()), builder));
}

// The carry set is an exploration's second operand segment, so a trim of it rewrites the
// segment sizes as a trimmed procedure call's are rewritten
void trimExploreSegments(ExplorePaths exploration, llvm::ArrayRef<size_t> kept, OperationState& state, mlir::OpBuilder& builder) {
    const int32_t keptCount = static_cast<int32_t>(kept.size());
    const int32_t endNodeCount = exploration.getEndNodes() ? 1 : 0;
    const int32_t importCount = static_cast<int32_t>(exploration.getHopImports().size());

    state.attributes.set(exploration.getOperandSegmentSizesAttrName(),
                         builder.getDenseI32ArrayAttr({1, keptCount, endNodeCount, importCount}));
}

void trimAttributes(Operation* op, llvm::ArrayRef<size_t> kept, OperationState& state, mlir::OpBuilder& builder) {
    if (Sort sort = dyn_cast<Sort>(op)) {
        renumberSortKeys(sort, kept, state, builder);
    } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(op)) {
        renumberEndColumn(exploration, kept, state, builder);
        trimExploreSegments(exploration, kept, state, builder);
    } else if (GroupAggregate groupAggregate = dyn_cast<GroupAggregate>(op)) {
        trimGroupAggregateKinds(groupAggregate, kept, state, builder);
    } else if (Collect collect = dyn_cast<Collect>(op)) {
        trimCollectAttributes(collect, kept, state, builder);
    } else if (CallProcedure call = dyn_cast<CallProcedure>(op)) {
        const int32_t inputCount = static_cast<int32_t>(call.getInputs().size());
        const int32_t keptCount = static_cast<int32_t>(kept.size());
        state.attributes.set(call.getOperandSegmentSizesAttrName(), builder.getDenseI32ArrayAttr({inputCount, keptCount}));
    }
}

void trimCarrySet(Operation* op, const CarrySetLayout& layout, llvm::ArrayRef<size_t> kept, mlir::OpBuilder& builder) {
    const Operation::operand_range operands = op->getOperands();
    const Operation::result_range results = op->getResults();

    llvm::SmallVector<Value> trimmedOperands;
    llvm::append_range(trimmedOperands, operands.take_front(layout._operandOffset));

    llvm::SmallVector<Type> trimmedTypes;
    llvm::append_range(trimmedTypes, results.take_front(layout._resultOffset).getTypes());

    for (const size_t carriedIndex : kept) {
        trimmedOperands.push_back(operands[layout._operandOffset + carriedIndex]);
        trimmedTypes.push_back(results[layout._resultOffset + carriedIndex].getType());
    }

    llvm::append_range(trimmedOperands, operands.take_back(layout._trailingOperandCount));

    OperationState state(op->getLoc(), op->getName());
    state.addOperands(trimmedOperands);
    state.addTypes(trimmedTypes);
    state.addAttributes(op->getAttrs());
    trimAttributes(op, kept, state, builder);

    const unsigned regionCount = op->getNumRegions();
    for (unsigned regionIndex = 0; regionIndex < regionCount; regionIndex++) {
        state.addRegion();
    }

    builder.setInsertionPoint(op);
    Operation* const trimmed = builder.create(state);

    for (unsigned regionIndex = 0; regionIndex < regionCount; regionIndex++) {
        trimmed->getRegion(regionIndex).takeBody(op->getRegion(regionIndex));
    }

    for (size_t resultIndex = 0; resultIndex < layout._resultOffset; resultIndex++) {
        results[resultIndex].replaceAllUsesWith(trimmed->getResult(resultIndex));
    }

    for (size_t keptIndex = 0; keptIndex < kept.size(); keptIndex++) {
        Value carried = results[layout._resultOffset + kept[keptIndex]];
        carried.replaceAllUsesWith(trimmed->getResult(layout._resultOffset + keptIndex));
    }
}

void eraseUnkeptYields(Yield yield, const llvm::SmallBitVector& keep, size_t firstIndex) {
    llvm::BitVector erased(yield.getNumOperands());
    for (size_t columnIndex = 0; columnIndex < erased.size(); columnIndex++) {
        if (!keep[firstIndex + columnIndex]) {
            erased.set(columnIndex);
        }
    }

    yield->eraseOperands(erased);
}

// The db.yield ending a factor region of a two-factor op - db.cross_product, db.hash_join.
Yield factorYield(Operation* op, unsigned factorIndex) {
    return cast<Yield>(op->getRegion(factorIndex).front().getTerminator());
}

// The results a two-factor op has to keep: the ones something reads, the ones the op
// itself matches on, and - since lowering sizes each side by a column of it - one
// row-carrying column per factor. False when that is every result, so the caller leaves
// the op alone.
bool selectKeptFactorColumns(Operation* op, llvm::SmallBitVector& keep) {
    Yield leftYield = factorYield(op, 0);
    Yield rightYield = factorYield(op, 1);
    const size_t leftCount = leftYield.getNumOperands();

    const Operation::result_range results = op->getResults();

    keep.resize(results.size());
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        if (!results[resultIndex].use_empty()) {
            keep.set(resultIndex);
        }
    }

    // A join reads its two key columns to match on whether or not anything downstream
    // reads them, the way a sort orders by its keys.
    if (HashJoin join = dyn_cast<HashJoin>(op)) {
        keep.set(join.getLeftKey());
        keep.set(leftCount + join.getRightKey());
    }

    keepRowCarryingColumn(leftYield.getColumns(), 0, keep);
    keepRowCarryingColumn(rightYield.getColumns(), leftCount, keep);

    return !keep.all();
}

void keptResultTypes(Operation* op, const llvm::SmallBitVector& keep, llvm::SmallVectorImpl<Type>& types) {
    const Operation::result_range results = op->getResults();
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        if (keep[resultIndex]) {
            types.push_back(results[resultIndex].getType());
        }
    }
}

// Where a factor's key column lands once that factor's unkept columns are gone: the kept
// columns of the factor ahead of it. firstIndex is where the factor's columns start among
// the op's results.
size_t trimmedKeyColumn(const llvm::SmallBitVector& keep, size_t firstIndex, size_t keyColumn) {
    size_t trimmed = 0;
    for (size_t columnIndex = 0; columnIndex < keyColumn; columnIndex++) {
        if (keep[firstIndex + columnIndex]) {
            trimmed++;
        }
    }

    return trimmed;
}

// Hands the two factors to the op built in its place, drops the columns their yields no
// longer name, and rewires every kept result. Shared by the cross product and the hash
// join, which differ only in the op the caller built.
void replaceWithTrimmedFactors(Operation* op,
                               Operation* trimmed,
                               const llvm::SmallBitVector& keep,
                               size_t leftCount) {
    Yield leftYield = factorYield(op, 0);
    Yield rightYield = factorYield(op, 1);

    trimmed->getRegion(0).takeBody(op->getRegion(0));
    trimmed->getRegion(1).takeBody(op->getRegion(1));

    eraseUnkeptYields(leftYield, keep, 0);
    eraseUnkeptYields(rightYield, keep, leftCount);

    const Operation::result_range results = op->getResults();
    size_t trimmedIndex = 0;
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        if (keep[resultIndex]) {
            results[resultIndex].replaceAllUsesWith(trimmed->getResult(trimmedIndex++));
        }
    }
}

// A product's results are the columns its two factors yield; a result nobody reads leaves
// the yield, and each factor keeps a row-carrying column to be sized by.
void trimCrossProduct(CrossProduct product, mlir::OpBuilder& builder) {
    Operation* const productOp = product.getOperation();

    llvm::SmallBitVector keep;
    if (!selectKeptFactorColumns(productOp, keep)) {
        return;
    }

    const size_t leftCount = factorYield(productOp, 0).getNumOperands();

    llvm::SmallVector<Type> trimmedTypes;
    keptResultTypes(productOp, keep, trimmedTypes);

    builder.setInsertionPoint(product);
    CrossProduct trimmed = builder.create<CrossProduct>(product.getLoc(), trimmedTypes);

    replaceWithTrimmedFactors(productOp, trimmed.getOperation(), keep, leftCount);
    productOp->erase();
}

// The product's sibling, with the two key columns kept on top of what is read and each
// renumbered into the yield the trim leaves behind.
void trimHashJoin(HashJoin join, mlir::OpBuilder& builder) {
    Operation* const joinOp = join.getOperation();

    llvm::SmallBitVector keep;
    if (!selectKeptFactorColumns(joinOp, keep)) {
        return;
    }

    const size_t leftCount = factorYield(joinOp, 0).getNumOperands();
    const size_t leftKey = trimmedKeyColumn(keep, 0, join.getLeftKey());
    const size_t rightKey = trimmedKeyColumn(keep, leftCount, join.getRightKey());

    llvm::SmallVector<Type> trimmedTypes;
    keptResultTypes(joinOp, keep, trimmedTypes);

    builder.setInsertionPoint(join);
    HashJoin trimmed = builder.create<HashJoin>(join.getLoc(), trimmedTypes, leftKey, rightKey);

    replaceWithTrimmedFactors(joinOp, trimmed.getOperation(), keep, leftCount);
    joinOp->erase();
}

// A lazy case hands its carried columns to every region as block arguments, so a column no
// region reads leaves the operands and the arguments of every region together. A column
// carried twice - the subject of `CASE t WHEN ...` beside the variable t - is read through
// its first argument. One row-carrying column stays for the lowering to size the CASE by.
void trimLazyCase(LazyCase caseOp) {
    const OperandRange carried = caseOp.getColumnsToFilter();
    const MutableArrayRef<Region> regions = caseOp.getBranches();

    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        const auto firstIt = llvm::find(carried, carried[carriedIndex]);
        const size_t firstIndex = static_cast<size_t>(firstIt - carried.begin());

        if (firstIndex == carriedIndex) {
            continue;
        }

        for (Region& region : regions) {
            Block& block = region.front();
            block.getArgument(carriedIndex).replaceAllUsesWith(block.getArgument(firstIndex));
        }
    }

    llvm::SmallBitVector keep(carried.size());
    for (Region& region : regions) {
        const Block::BlockArgListType arguments = region.front().getArguments();

        for (size_t argumentIndex = 0; argumentIndex < arguments.size(); argumentIndex++) {
            if (!arguments[argumentIndex].use_empty()) {
                keep.set(argumentIndex);
            }
        }
    }

    keepRowCarryingColumn(carried, 0, keep);

    if (keep.all()) {
        return;
    }

    llvm::BitVector erased(carried.size());
    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        if (!keep[carriedIndex]) {
            erased.set(carriedIndex);
        }
    }

    for (Region& region : regions) {
        region.front().eraseArguments(erased);
    }

    caseOp->eraseOperands(erased);
}

// The ops over the elements of a list repeat every carried column once per element, so a
// column the body does not read leaves the operands and the body's arguments together. A
// constant list is laid out over the rows of a carried column, so one row-carrying column
// stays for it.
void trimElementCarrySet(Operation* op, Value source, OperandRange carried, Block& body) {
    const size_t argumentOffset = body.getNumArguments() - carried.size();

    llvm::SmallBitVector keep(carried.size());
    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        if (!body.getArgument(argumentOffset + carriedIndex).use_empty()) {
            keep.set(carriedIndex);
        }
    }

    if (::db::yieldsConstantColumn(source)) {
        keepRowCarryingColumn(carried, 0, keep);
    }

    if (keep.all()) {
        return;
    }

    const size_t operandOffset = carried.getBeginOperandIndex();

    llvm::BitVector erasedOperands(op->getNumOperands());
    llvm::BitVector erasedArguments(body.getNumArguments());
    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        if (!keep[carriedIndex]) {
            erasedOperands.set(operandOffset + carriedIndex);
            erasedArguments.set(argumentOffset + carriedIndex);
        }
    }

    body.eraseArguments(erasedArguments);
    op->eraseOperands(erasedOperands);
}

bool readsCarriedColumnsInItsRegions(Operation* op) {
    return isa<LazyCase, ListComprehension, ListPredicate, Reduce>(op);
}

struct TrimVisit {
    Operation* _op {nullptr};
    bool _regionsVisited {false};
};

void pushNestedOps(Operation* op, llvm::SmallVectorImpl<TrimVisit>& worklist) {
    for (Region& region : op->getRegions()) {
        for (Block& block : region) {
            for (Operation& nested : block) {
                worklist.push_back(TrimVisit {._op = &nested});
            }
        }
    }
}

// Each block is swept backwards, so a reader is trimmed before the ops feeding it. An op
// reading its carried columns inside its regions is trimmed after what they hold; a hop or
// a two-factor op only reads what its regions yield, so it is trimmed before them.
void collectTrimOrder(Operation* root, llvm::SmallVectorImpl<Operation*>& order) {
    llvm::SmallVector<TrimVisit> worklist;
    pushNestedOps(root, worklist);

    while (!worklist.empty()) {
        const TrimVisit visit = worklist.pop_back_val();
        Operation* const op = visit._op;

        if (visit._regionsVisited) {
            order.push_back(op);
            continue;
        }

        if (readsCarriedColumnsInItsRegions(op)) {
            worklist.push_back(TrimVisit {._op = op, ._regionsVisited = true});
        } else if (trimsColumns(op)) {
            order.push_back(op);
        }

        pushNestedOps(op, worklist);
    }
}

void trimCarriedColumns(Operation* op, mlir::OpBuilder& builder) {
    CarrySetLayout layout;
    const bool carries = matchCarrySetLayout(op, layout);
    bioassert(carries, "A trimming op that is neither a cross product nor a join has a carry set");

    llvm::SmallVector<size_t> kept;
    selectKeptCarriedColumns(op, layout, kept);

    if (kept.size() == carriedCount(op, layout)) {
        return;
    }

    trimCarrySet(op, layout, kept, builder);
    op->erase();
}

void trimUnreadColumnsOf(Operation* op, mlir::OpBuilder& builder) {
    if (LazyCase caseOp = dyn_cast<LazyCase>(op)) {
        trimLazyCase(caseOp);
    } else if (ListComprehension comprehension = dyn_cast<ListComprehension>(op)) {
        trimElementCarrySet(op, comprehension.getSource(), comprehension.getColumnsToFilter(), comprehension.getBody().front());
    } else if (ListPredicate predicate = dyn_cast<ListPredicate>(op)) {
        trimElementCarrySet(op, predicate.getSource(), predicate.getColumnsToFilter(), predicate.getBody().front());
    } else if (Reduce reduce = dyn_cast<Reduce>(op)) {
        trimElementCarrySet(op, reduce.getSource(), reduce.getColumnsToFilter(), reduce.getBody().front());
    } else if (CrossProduct product = dyn_cast<CrossProduct>(op)) {
        trimCrossProduct(product, builder);
    } else if (HashJoin join = dyn_cast<HashJoin>(op)) {
        trimHashJoin(join, builder);
    } else {
        trimCarriedColumns(op, builder);
    }
}

struct TrimUnreadColumns : public impl::TrimUnreadColumnsBase<TrimUnreadColumns> {
    void runOnOperation() override {
        llvm::SmallVector<Operation*> order;
        collectTrimOrder(getOperation(), order);

        mlir::OpBuilder builder(&getContext());
        for (Operation* const op : order) {
            trimUnreadColumnsOf(op, builder);
        }
    }
};

// A property equality fuses into the scan and every other conjunct of the mask is rebuilt
// over the fused rows, so the whole cone is cloned or dropped: it must read nothing but
// the scanned column, and nothing outside it may read a part of it.
struct PropertyValueScanChain {
    ScanSource _source;
    MaskCone _cone;
    StringAttr _property;
    TypedAttr _value;
    llvm::SmallVector<Value> _residual;
};

// eq(get_node_properties(scan, property), constant), with the constant on either side
bool matchPropertyEquality(EqOp equality, Value scanColumn, PropertyValueScanChain& chain) {
    const Value lhs = equality.getLhs();
    const Value rhs = equality.getRhs();

    GetNodeProperties read = lhs.getDefiningOp<GetNodeProperties>();
    Value constantSide = rhs;
    if (!read) {
        read = rhs.getDefiningOp<GetNodeProperties>();
        constantSide = lhs;
    }

    if (!read || read.getInputNodes() != scanColumn) {
        return false;
    }

    ConstantOp constant = constantSide.getDefiningOp<ConstantOp>();
    if (!constant) {
        return false;
    }

    const TypedAttr literal = dyn_cast<TypedAttr>(constant.getValue());
    if (!literal || !storage::isPropertyScanLiteral(literal)) {
        return false;
    }

    chain._property = read.getPropertyAttr();
    chain._value = literal;
    return true;
}

// The conjuncts of a mask, as an `and` tree spells them: the predicates that all have to
// hold, one of which can become the fused scan while the others stay a filter.
void collectConjuncts(Value mask, llvm::SmallVectorImpl<Value>& conjuncts) {
    llvm::SmallVector<Value> pending {mask};
    while (!pending.empty()) {
        const Value conjunct = pending.pop_back_val();

        if (AndOp conjunction = conjunct.getDefiningOp<AndOp>()) {
            pending.push_back(conjunction.getRhs());
            pending.push_back(conjunction.getLhs());
        } else {
            conjuncts.push_back(conjunct);
        }
    }
}

bool maskConeIsPrivateTo(const MaskCone& cone, FilterOp filter) {
    llvm::SmallPtrSet<Operation*, 8> coneOps;
    for (Operation* const coneOp : cone._ops) {
        coneOps.insert(coneOp);
    }

    Operation* const filterOp = filter.getOperation();
    for (Operation* const coneOp : cone._ops) {
        for (Operation* const user : coneOp->getUsers()) {
            if (user != filterOp && !coneOps.contains(user)) {
                return false;
            }
        }
    }

    return true;
}

bool matchPropertyValueScanChain(FilterOp filter, PropertyValueScanChain& chain) {
    if (!matchSoleScanSource(filter, chain._source)) {
        return false;
    }

    chain._cone = collectMaskCone(filter.getMask());

    const MaskCone& cone = chain._cone;
    const bool readsTheScannedColumnAlone = cone._inputs.size() == 1 && cone._inputs.front() == chain._source._column;
    if (!readsTheScannedColumnAlone || !maskConeIsPrivateTo(cone, filter)) {
        return false;
    }

    llvm::SmallVector<Value> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    // The first property equality in the mask wins, not the most selective one: there are
    // no column statistics to choose with, so `n.gender = 'M' AND n.ssn = '...'` fuses the
    // unselective conjunct and leaves the selective one a residual filter.
    for (size_t candidate = 0; candidate < conjuncts.size(); candidate++) {
        EqOp equality = conjuncts[candidate].getDefiningOp<EqOp>();
        if (!equality || !matchPropertyEquality(equality, chain._source._column, chain)) {
            continue;
        }

        for (size_t other = 0; other < conjuncts.size(); other++) {
            if (other == candidate) {
                continue;
            }

            // Only a conjunct the cone built can be rebuilt over the fused rows.
            Operation* const conjunctDef = conjuncts[other].getDefiningOp();
            if (!conjunctDef || !isMaskComputeOp(conjunctDef)) {
                return false;
            }

            chain._residual.push_back(conjuncts[other]);
        }

        return true;
    }

    return false;
}

bool isUntypedNullMask(Value mask) {
    const auto columnType = dyn_cast<ColumnType>(mask.getType());
    if (!columnType) {
        return false;
    }

    const auto nullableType = dyn_cast<storage::NullableType>(columnType.getType());
    return nullableType && isa<mlir::NoneType>(nullableType.getValueType());
}

// Rebuilds the conjuncts the fused scan does not carry over its rows, as the mask of the
// filter that stays. The clone reads the fused column wherever the original read the scan.
Value cloneResidualMask(llvm::ArrayRef<Value> residual,
                        Value scanColumn,
                        Value fused,
                        Type maskType,
                        mlir::Location loc,
                        mlir::OpBuilder& builder) {
    MaskCone cone;
    llvm::SmallPtrSet<Operation*, 8> visited;
    for (const Value conjunct : residual) {
        collectConePostOrder(conjunct, isMaskComputeOp, visited, cone);
    }

    mlir::IRMapping mapping;
    mapping.map(scanColumn, fused);
    for (Operation* const coneOp : cone._ops) {
        builder.clone(*coneOp, mapping);
    }

    Value mask = mapping.lookup(residual.front());
    for (const Value conjunct : residual.drop_front()) {
        const Value clonedConjunct = mapping.lookup(conjunct);

        // null AND null is null, so a second null conjunct adds nothing to the mask
        const bool bothUnknown = isUntypedNullMask(mask) && isUntypedNullMask(clonedConjunct);
        if (bothUnknown) {
            continue;
        }

        AndOp conjunction = builder.create<AndOp>(loc, maskType, mask, clonedConjunct);
        mask = conjunction.getResult();
    }

    return mask;
}

void fuseScanByPropertyValue(FilterOp filter, const PropertyValueScanChain& chain, mlir::OpBuilder& builder) {
    const mlir::Location loc = filter.getLoc();
    const Type nodeColumnType = chain._source._column.getType();

    builder.setInsertionPoint(filter);
    ScanNodesByPropertyValue scan = builder.create<ScanNodesByPropertyValue>(loc,
                                                                             nodeColumnType,
                                                                             chain._property,
                                                                             chain._value,
                                                                             chain._source._labels);

    Value fused = scan.getResult();
    if (!chain._residual.empty()) {
        const Value mask = cloneResidualMask(chain._residual,
                                             chain._source._column,
                                             fused,
                                             filter.getMask().getType(),
                                             loc,
                                             builder);

        const llvm::SmallVector<Type> resultTypes {nodeColumnType};
        const llvm::SmallVector<Value> columns {fused};
        FilterOp residualFilter = builder.create<FilterOp>(loc, resultTypes, mask, columns);
        fused = residualFilter.getResult(0);
    }

    replaceFilterWithSource(filter, fused, chain._source._op, chain._cone);
}

struct FuseScanByPropertyValue : public impl::FuseScanByPropertyValueBase<FuseScanByPropertyValue> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runFilterPass<PropertyValueScanChain>(getOperation(),
                                              matchPropertyValueScanChain,
                                              fuseScanByPropertyValue,
                                              builder);
    }
};

// The ops whose results hold the rows of their operands selected, reordered or repeated as
// a whole, values untouched, so a property read of an operand and carried through is the
// column a read of the result would have produced. group_aggregate and collect are not
// among them - a group's rows are not its input's - and neither is remove_duplicates,
// whose dedup key is every column it carries, so a wider carry set drops different rows.
bool mapsRowsThrough(Operation* op) {
    return isEdgeHop(op) || isa<FilterOp, Unwind, Limit, Skip, Sort>(op);
}

// The operand whose rows a result's rows are drawn from, if any: each carried column comes
// back at its own position, and a hop's input re-surfaces as srcids, or tgtids when it
// walks backwards.
bool matchRowSourceOperand(Operation* op, const CarrySetLayout& layout, size_t resultIndex, size_t& operandIndex) {
    if (resultIndex >= layout._resultOffset) {
        operandIndex = layout._operandOffset + (resultIndex - layout._resultOffset);
        return true;
    } else if (isEdgeHop(op)) {
        if (resultIndex == hopInputResult(op)) {
            operandIndex = 0;
            return true;
        }
    }

    return false;
}

bool matchPropertyRead(Operation* op, StringAttr& property, bool& nodeProperty) {
    if (GetNodeProperties read = dyn_cast<GetNodeProperties>(op)) {
        property = read.getPropertyAttr();
        nodeProperty = true;
        return true;
    } else if (GetEdgeProperties read = dyn_cast<GetEdgeProperties>(op)) {
        property = read.getPropertyAttr();
        nodeProperty = false;
        return true;
    }

    return false;
}

bool matchPropertyWrite(Operation* op, StringAttr& property, bool& nodeProperty) {
    if (SetNodeProperty write = dyn_cast<SetNodeProperty>(op)) {
        property = write.getPropertyAttr();
        nodeProperty = true;
        return true;
    } else if (SetEdgeProperty write = dyn_cast<SetEdgeProperty>(op)) {
        property = write.getPropertyAttr();
        nodeProperty = false;
        return true;
    }

    return false;
}

// Widens an op's carry set by one column and hands back the result it comes out as. A carry
// set is the trailing operands and the trailing results of the op holding it, so the column
// appends to both; an op's arity is fixed once built, hence the rebuild.
Value appendCarriedColumn(Operation* op, Value column, mlir::RewriterBase& rewriter) {
    llvm::SmallVector<Value> operands;
    llvm::append_range(operands, op->getOperands());
    operands.push_back(column);

    llvm::SmallVector<Type> types;
    llvm::append_range(types, op->getResultTypes());
    types.push_back(column.getType());

    OperationState state(op->getLoc(), op->getName());
    state.addOperands(operands);
    state.addTypes(types);
    state.addAttributes(op->getAttrs());

    rewriter.setInsertionPoint(op);
    Operation* const widened = rewriter.create(state);

    rewriter.replaceOp(op, widened->getResults().drop_back());

    return widened->getResults().back();
}

Value readStandingBefore(Value column, StringAttr property, bool nodeProperty, Operation* useSite) {
    for (Operation* const user : column.getUsers()) {
        StringAttr userProperty;
        bool userReadsNodes = false;
        if (user == useSite || !matchPropertyRead(user, userProperty, userReadsNodes)) {
            continue;
        }

        const bool readsTheSameProperty = userReadsNodes == nodeProperty && userProperty == property;
        const bool standsBeforeTheUse = user->getBlock() == useSite->getBlock() && user->isBeforeInBlock(useSite);

        if (readsTheSameProperty && standsBeforeTheUse) {
            return user->getResult(0);
        }
    }

    return {};
}

Value carryThrough(Operation* op, Value column, mlir::RewriterBase& rewriter) {
    CarrySetLayout layout;
    const bool carries = matchCarrySetLayout(op, layout);
    bioassert(carries, "A row-mapping op has a carry set");

    const size_t carriedColumnCount = carriedCount(op, layout);
    for (size_t carriedIndex = 0; carriedIndex < carriedColumnCount; carriedIndex++) {
        if (op->getOperand(layout._operandOffset + carriedIndex) == column) {
            return op->getResult(layout._resultOffset + carriedIndex);
        }
    }

    return appendCarriedColumn(op, column, rewriter);
}

// The property columns reuse_property_reads carried down to each row column, so a later read
// of those rows stops where an earlier one left the property rather than climbing back to
// the read it came from
class CarriedPropertyColumns {
public:
    Value find(Value rows, StringAttr property, bool nodeProperty) const {
        const auto rowsIt = _columns.find(rows);
        if (rowsIt == _columns.end()) {
            return {};
        }

        for (const CarriedProperty& carried : rowsIt->second) {
            if (carried._property == property && carried._nodeProperty == nodeProperty) {
                return carried._column;
            }
        }

        return {};
    }

    void add(Value rows, StringAttr property, bool nodeProperty, Value column) {
        _columns[rows].push_back(CarriedProperty {property, nodeProperty, column});
    }

    // Moves what was carried to @param replacedResults, the results of an op since widened
    // into @param widened, onto the results of @param widened at the same positions
    void rebind(llvm::ArrayRef<Value> replacedResults, Operation* widened) {
        llvm::DenseMap<Value, size_t> replacedIndices;
        for (size_t index = 0; index < replacedResults.size(); index++) {
            replacedIndices[replacedResults[index]] = index;
        }

        for (size_t index = 0; index < replacedResults.size(); index++) {
            const auto rowsIt = _columns.find(replacedResults[index]);
            if (rowsIt == _columns.end()) {
                continue;
            }

            llvm::SmallVector<CarriedProperty, 1> carried = rowsIt->second;
            _columns.erase(rowsIt);

            for (CarriedProperty& property : carried) {
                const auto columnIt = replacedIndices.find(property._column);
                if (columnIt != replacedIndices.end()) {
                    property._column = widened->getResult(columnIt->second);
                }
            }

            _columns[widened->getResult(index)] = carried;
        }
    }

private:
    struct CarriedProperty {
        StringAttr _property;
        bool _nodeProperty {false};
        Value _column;
    };

    llvm::DenseMap<Value, llvm::SmallVector<CarriedProperty, 1>> _columns;
};

// The column holding `property` for the rows of `column`, taken from a read already
// standing before `useSite` and carried down through the ops in between when that read was
// taken further up the chain. Null when there is none: this adds no read of its own.
Value propertyColumnOf(Value column,
                       StringAttr property,
                       bool nodeProperty,
                       Operation* useSite,
                       CarriedPropertyColumns& carriedColumns,
                       mlir::RewriterBase& rewriter) {
    llvm::SmallVector<Operation*> carriers;
    llvm::SmallVector<size_t> carriedRowsResults;
    Value rows = column;

    Value propertyColumn = readStandingBefore(rows, property, nodeProperty, useSite);
    if (!propertyColumn) {
        propertyColumn = carriedColumns.find(rows, property, nodeProperty);
    }

    while (!propertyColumn) {
        Operation* const def = rows.getDefiningOp();
        if (!def || !mapsRowsThrough(def)) {
            return {};
        }

        CarrySetLayout layout;
        const bool carries = matchCarrySetLayout(def, layout);
        bioassert(carries, "A row-mapping op has a carry set");

        size_t sourceOperandIndex = 0;
        const size_t resultIndex = cast<OpResult>(rows).getResultNumber();
        if (!matchRowSourceOperand(def, layout, resultIndex, sourceOperandIndex)) {
            return {};
        }

        carriers.push_back(def);
        carriedRowsResults.push_back(resultIndex);
        rows = def->getOperand(sourceOperandIndex);

        propertyColumn = readStandingBefore(rows, property, nodeProperty, def);
        if (!propertyColumn) {
            propertyColumn = carriedColumns.find(rows, property, nodeProperty);
        }
    }

    for (size_t carrierIndex = carriers.size(); carrierIndex > 0; carrierIndex--) {
        Operation* const carrier = carriers[carrierIndex - 1];
        const llvm::SmallVector<Value> carrierResults(carrier->getResults());

        propertyColumn = carryThrough(carrier, propertyColumn, rewriter);

        Operation* const carrying = propertyColumn.getDefiningOp();
        if (carrying != carrier) {
            carriedColumns.rebind(carrierResults, carrying);
        }

        carriedColumns.add(carrying->getResult(carriedRowsResults[carrierIndex - 1]), property, nodeProperty, propertyColumn);
    }

    return propertyColumn;
}

struct ReusePropertyReads : public impl::ReusePropertyReadsBase<ReusePropertyReads> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        llvm::SmallVector<Operation*> reads;
        llvm::DenseSet<Attribute> writtenNodeProperties;
        llvm::DenseSet<Attribute> writtenEdgeProperties;
        root->walk([&](Operation* op) {
            StringAttr writtenProperty;
            bool writesNodes = false;

            if (isa<GetNodeProperties, GetEdgeProperties>(op)) {
                reads.push_back(op);
            } else if (matchPropertyWrite(op, writtenProperty, writesNodes)) {
                if (writesNodes) {
                    writtenNodeProperties.insert(writtenProperty);
                } else {
                    writtenEdgeProperties.insert(writtenProperty);
                }
            }
        });

        mlir::IRRewriter rewriter(&getContext());
        CarriedPropertyColumns carriedColumns;
        for (Operation* const read : reads) {
            StringAttr property;
            bool nodeProperty = false;
            const bool isPropertyRead = matchPropertyRead(read, property, nodeProperty);
            bioassert(isPropertyRead, "A collected op is a property read");

            // A read standing before this one saw what the property held before the write,
            // and a projection behind a SET reads what the statement wrote
            const llvm::DenseSet<Attribute>& written = nodeProperty ? writtenNodeProperties : writtenEdgeProperties;
            if (written.contains(property)) {
                continue;
            }

            const Value reused = propertyColumnOf(read->getOperand(0), property, nodeProperty, read, carriedColumns, rewriter);
            if (!reused) {
                continue;
            }

            read->getResult(0).replaceAllUsesWith(reused);
            read->erase();
        }
    }
};

// One side of a join: the factor supplying its key, the product column the key is read
// from, the ops computing one from the other, and - once sunk - where the key sits in the
// factor's yield.
struct JoinKeySide {
    Region* _factor {nullptr};
    Value _key;
    Value _column;
    MaskCone _cone;
};

// A cross product cut by one equality between a column of each factor, and the filter
// applying it: what a hash join is made of. _buildsTheLeftFactor says which way round the
// two go into the join, whose right factor is the built side.
struct EqualityCross {
    CrossProduct _product {nullptr};
    EqOp _equality {nullptr};
    FilterOp _filter {nullptr};
    JoinKeySide _left;
    JoinKeySide _right;
    bool _buildsTheLeftFactor {false};
};

// The property a one-op key cone reads, null for any other cone. A property column's
// value type is resolved from the name during lowering, so two keys read from one name
// are two columns of one type.
StringAttr conePropertyName(const MaskCone& cone) {
    if (cone._ops.size() != 1) {
        return nullptr;
    }

    Operation* const read = cone._ops.front();
    if (GetNodeProperties nodeRead = dyn_cast<GetNodeProperties>(read)) {
        return nodeRead.getPropertyAttr();
    } else if (GetEdgeProperties edgeRead = dyn_cast<GetEdgeProperties>(read)) {
        return edgeRead.getPropertyAttr();
    }

    return nullptr;
}

// Whether the two sides' keys are provably columns of one type. The join matches a build
// key against a probe key by the bytes each serializes to, so two columns that could
// serialize differently - an integer property against a string one, a node against a
// number - would answer a comparison the equality did not. A db column type is concrete
// only for the entity and metadata columns; a column typed none carries no type at all
// until lowering resolves it, so two of those are only provably alike when both are read
// from one property name.
bool keysShareAColumnType(const JoinKeySide& left, const JoinKeySide& right) {
    if (left._cone._ops.empty() && right._cone._ops.empty()) {
        const Type leftType = left._key.getType();
        const bool typeIsResolved = !isa<mlir::NoneType>(cast<ColumnType>(leftType).getType());

        return typeIsResolved && leftType == right._key.getType();
    }

    const StringAttr leftProperty = conePropertyName(left._cone);

    return leftProperty && leftProperty == conePropertyName(right._cone);
}

// Reads into side the one product column a key is computed over, along with the ops
// computing one from the other. False when the key reads no column, more than one, or one
// a different cross product made - product names the one already matched, null for the
// first side, and is set to the one this side found.
bool matchKeySide(Value key, JoinKeySide& side, CrossProduct& product) {
    side._key = key;
    side._cone = collectMaskCone(key);

    if (side._cone._inputs.size() != 1) {
        return false;
    }

    side._column = side._cone._inputs.front();

    CrossProduct keyProduct = side._column.getDefiningOp<CrossProduct>();
    if (!keyProduct || (product && keyProduct != product)) {
        return false;
    }

    product = keyProduct;

    return true;
}

// Whether nothing but the equality reads the ops rebuilding a key. The cone is sunk into
// the factor and dropped from here, so a reader left behind would lose its operand - the
// exception being the filter carrying the key itself, which the join yields among its own
// columns and hands to that reader instead.
bool keyConeIsPrivateTo(const JoinKeySide& side, EqOp equality, FilterOp filter) {
    llvm::SmallPtrSet<Operation*, 8> coneOps;
    for (Operation* const coneOp : side._cone._ops) {
        coneOps.insert(coneOp);
    }

    Operation* const equalityOp = equality.getOperation();
    Operation* const filterOp = filter.getOperation();
    Operation* const keyOp = side._key.getDefiningOp();

    for (Operation* const coneOp : side._cone._ops) {
        const bool holdsTheKey = coneOp == keyOp;

        for (Operation* const user : coneOp->getUsers()) {
            const bool isTheEquality = user == equalityOp;
            const bool isAnotherConeOp = coneOps.contains(user);
            const bool carriesTheKey = holdsTheKey && user == filterOp;

            if (!isTheEquality && !isAnotherConeOp && !carriesTheKey) {
                return false;
            }
        }
    }

    return true;
}

// Whether the product's rows reach nothing but the equality and the filter it masks. The
// join emits the rows the filter kept, so a reader of the product's own rows would go
// from seeing every pair to seeing only the matching ones.
bool productRowsReachOnly(CrossProduct product, const EqualityCross& match) {
    EqOp equality = match._equality;
    FilterOp filter = match._filter;

    llvm::SmallPtrSet<Operation*, 8> readers;
    readers.insert(equality.getOperation());
    readers.insert(filter.getOperation());

    const JoinKeySide* const sides[] = {&match._left, &match._right};
    for (const JoinKeySide* const side : sides) {
        for (Operation* const coneOp : side->_cone._ops) {
            readers.insert(coneOp);
        }
    }

    for (const Value column : product.getResults()) {
        for (Operation* const user : column.getUsers()) {
            if (!readers.contains(user)) {
                return false;
            }
        }
    }

    return true;
}

// Whether every column the filter carries is one the join hands back as a result of its
// own: a column the product made, or a key, which the join yields beside the columns its
// factor already had. One computed from them outside was computed over the rows the join
// no longer produces.
bool carriesJoinColumnsOnly(FilterOp filter, const EqualityCross& match) {
    for (const Value carried : filter.getColumnsToFilter()) {
        const bool isAProductColumn = carried.getDefiningOp<CrossProduct>() == match._product;
        const bool isAKey = carried == match._left._key || carried == match._right._key;

        if (!isAProductColumn && !isAKey) {
            return false;
        }
    }

    return true;
}

// Whether a factor's rows are the product of two relations'. The built side is buffered
// whole while the probed side streams a chunk at a time, so a factor holding a product -
// which is where the cascade of a comma pattern puts one - is the side to probe: its rows
// multiply where a scan or a traversal contributes them linearly.
bool factorHoldsAProduct(Region& factor) {
    const WalkResult walked = factor.walk([](Operation* op) {
        if (isa<CrossProduct, HashJoin>(op)) {
            return WalkResult::interrupt();
        }

        return WalkResult::advance();
    });

    return walked.wasInterrupted();
}

// The label set a scan's label names stand for, resolved against the schema.
void collectScanLabels(ArrayAttr labelNames, const ::db::GraphMetadata& metadata, ::db::LabelSet& labels) {
    const ::db::LabelMap& labelMap = metadata.labels();
    for (const Attribute labelAttr : labelNames) {
        const llvm::StringRef name = cast<StringAttr>(labelAttr).getValue();
        const std::optional<::db::LabelID> label = labelMap.get(std::string_view(name.data(), name.size()));

        if (label) {
            labels.set(*label);
        }
    }
}

// The label set the rows of one side are scanned under, when the db level knows one: the
// labels of a by-label node scan, and none at all otherwise - every node of the graph,
// which is what the v2 planner estimates a variable with no label constraint at.
void keySideLabels(const JoinKeySide& side,
                   size_t factorFirstResult,
                   const ::db::GraphMetadata& metadata,
                   ::db::LabelSet& labels) {
    Yield yield = cast<Yield>(side._factor->front().getTerminator());
    const size_t columnIndex = cast<OpResult>(side._column).getResultNumber() - factorFirstResult;

    ScanNodesByLabel scan = yield.getColumns()[columnIndex].getDefiningOp<ScanNodesByLabel>();
    if (!scan) {
        return;
    }

    collectScanLabels(scan.getLabels(), metadata, labels);
}

// The row budget a limit downstream of the cut puts on the rows the join would emit, and
// zero when none governs them. A product stops as soon as the budget is met, where the
// join reads its whole build side before it can emit a row, which is what tips a small
// budget back towards the product.
uint64_t limitOverTheCut(FilterOp filter) {
    llvm::SmallVector<Operation*, 8> pending;
    llvm::SmallPtrSet<Operation*, 8> visited;
    for (const Value column : filter.getFilteredColumns()) {
        for (Operation* const user : column.getUsers()) {
            if (visited.insert(user).second) {
                pending.push_back(user);
            }
        }
    }

    while (!pending.empty()) {
        Operation* const op = pending.pop_back_val();
        if (Limit limit = dyn_cast<Limit>(op)) {
            return limit.getCount();
        }

        for (const Value result : op->getResults()) {
            for (Operation* const user : result.getUsers()) {
                if (visited.insert(user).second) {
                    pending.push_back(user);
                }
            }
        }
    }

    return 0;
}

// Whether the cut is better left as the product it stands as. The join reads its build
// side whole before emitting a row and indexes every key it holds, which a small product
// - or a small limit over a large one - never repays, so the shape alone does not settle
// which form to run: this is the v2 planner's verdict (ReadStmtGenerator::
// shouldPlaceValueHashJoin), answered by the same CardinalityEstimation over the node
// counts of each side's labels.
bool prefersTheProduct(const EqualityCross& match, const DBPassContext& context) {
    if (context._forcesHashJoin) {
        return false;
    } else if (!context._usesHashJoin) {
        return true;
    } else if (!context._view) {
        return false;
    }

    const ::db::GraphView& view = *context._view;
    const ::db::GraphMetadata& metadata = view.metadata();

    CrossProduct product = match._product;
    const size_t leftFactorColumns = factorYieldColumns(product.getLeftFactor()).size();

    ::db::LabelSet leftLabels;
    ::db::LabelSet rightLabels;
    keySideLabels(match._left, 0, metadata, leftLabels);
    keySideLabels(match._right, leftFactorColumns, metadata, rightLabels);

    const ::db::CardinalityEstimation estimation(view);

    return estimation.shouldPreferCartesian(leftLabels, rightLabels, limitOverTheCut(match._filter));
}

// What the db level puts on rows it cannot count - the figure the v2 planner reads a
// yielded item at.
constexpr size_t uncountableRows = 10;

size_t multiplySaturating(size_t rows, size_t factor) {
    constexpr size_t rowCeiling = std::numeric_limits<size_t>::max();
    if (factor != 0 && rows > rowCeiling / factor) {
        return rowCeiling;
    }

    return rows * factor;
}

// The labels of whichever of the four by-label edge scans this is, and null for any other
// op. They all read the edges hanging off the nodes the labels select, so they are counted
// the same way whichever endpoint carries them.
ArrayAttr byLabelEdgeScanLabels(Operation* op) {
    if (ScanOutEdgesByLabelSrc byLabel = dyn_cast<ScanOutEdgesByLabelSrc>(op)) {
        return byLabel.getLabels();
    } else if (ScanInEdgesByLabelTgt byLabel = dyn_cast<ScanInEdgesByLabelTgt>(op)) {
        return byLabel.getLabels();
    } else if (ScanOutEdgesByLabelTgt byLabel = dyn_cast<ScanOutEdgesByLabelTgt>(op)) {
        return byLabel.getLabels();
    } else if (ScanInEdgesByLabelSrc byLabel = dyn_cast<ScanInEdgesByLabelSrc>(op)) {
        return byLabel.getLabels();
    }

    return nullptr;
}

// The rows an op seeds a factor with, and nothing at all when it seeds none - a fetch, a
// filter or a hop reads a column rather than making one. A listed set of IDs is its own
// count, a literal list one row per element, a by-label scan what the graph holds under
// those labels, and a property scan the whole node count: selectivity is not something
// the db level knows.
std::optional<size_t> estimateSourceRows(Operation* op,
                                         const ::db::GraphMetadata& metadata,
                                         const ::db::CardinalityEstimation& estimation) {
    if (ConstScanNodes constScan = dyn_cast<ConstScanNodes>(op)) {
        return constScan.getNodeIDs().size();
    } else if (UnwindConst unwind = dyn_cast<UnwindConst>(op)) {
        return unwind.getElements().size();
    } else if (ScanNodesByLabel byLabel = dyn_cast<ScanNodesByLabel>(op)) {
        ::db::LabelSet labels;
        collectScanLabels(byLabel.getLabels(), metadata, labels);

        return estimation.estimateNodeCount(labels);
    } else if (isa<ScanNodes, ScanNodesByPropertyValue>(op)) {
        return estimation.estimateNodeCount(::db::LabelSet {});
    } else if (const ArrayAttr scanLabels = byLabelEdgeScanLabels(op)) {
        const size_t nodeCount = estimation.estimateNodeCount(::db::LabelSet {});
        if (nodeCount == 0) {
            return 0;
        }

        ::db::LabelSet labels;
        collectScanLabels(scanLabels, metadata, labels);

        // The edges hanging off those nodes, read as the by-label scan and hop it fused
        // were: the node count the labels select, at the graph's average degree.
        return multiplySaturating(estimation.estimateNodeCount(labels), estimation.estimateEdgeCount()) / nodeCount;
    } else if (ScanEdgesByType byType = dyn_cast<ScanEdgesByType>(op)) {
        if (byType.getEdgeTypes().empty()) {
            return 0;
        }

        return estimation.estimateEdgeCount();
    } else if (isa<ScanEdges>(op)) {
        return estimation.estimateEdgeCount();
    }

    return std::nullopt;
}

// The ratio an op multiplies the rows it reads by, and nothing when it hands them on as
// they come. A hop makes the graph's average degree of rows per row it reads - all the db
// level can say of a hop out of a node it cannot name - and an undirected one walks both
// directions, so twice that. It is a ratio rather than a count because a graph with fewer
// edges than nodes has a hop shrink its input rather than grow it. An unwind makes a row
// per element of each row's list, whose length is a property of the rows themselves: no
// statistic of the graph measures it, so it reads at the figure an uncountable source
// does - where a literal list, whose elements are in the IR, is counted exactly instead.
struct RowMultiplier {
    size_t _numerator {1};
    size_t _denominator {1};
};

std::optional<RowMultiplier> estimateRowMultiplier(Operation* op, const ::db::CardinalityEstimation& estimation) {
    if (isa<Unwind>(op)) {
        return RowMultiplier {uncountableRows, 1};
    }

    const bool walksOneDirection = isa<GetOutEdges,
                                       GetInEdges,
                                       GetOutEdgesByType,
                                       GetInEdgesByType,
                                       GetOutEdgesByLabel,
                                       GetInEdgesByLabel,
                                       GetOutEdgesByTypeAndLabel,
                                       GetInEdgesByTypeAndLabel>(op);
    const bool walksBoth = isa<GetEdges>(op);
    if (!walksOneDirection && !walksBoth) {
        return std::nullopt;
    }

    const bool isByTypeHop = isa<GetOutEdgesByType, GetInEdgesByType, GetOutEdgesByTypeAndLabel, GetInEdgesByTypeAndLabel>(op);
    const bool walksNoEdge = isByTypeHop && op->getAttrOfType<ArrayAttr>("edge_types").empty();
    if (walksNoEdge) {
        return RowMultiplier {0, 1};
    }

    const size_t nodeCount = estimation.estimateNodeCount(::db::LabelSet {});
    if (nodeCount == 0) {
        return std::nullopt;
    }

    const size_t edgeCount = estimation.estimateEdgeCount();

    return RowMultiplier {walksBoth ? 2 * edgeCount : edgeCount, nodeCount};
}

// The rows a factor makes: the product of what its sources seed it with, a factor crossing
// two of them holding a row per pair, grown or shrunk by every hop and unwind reading from
// one. A factor seeded from somewhere the db level cannot count - a procedure, a load -
// counts as an uncountable source's rows.
size_t estimateFactorRows(Region& factor,
                          const ::db::GraphMetadata& metadata,
                          const ::db::CardinalityEstimation& estimation) {
    bool countedASource = false;
    size_t rows = 1;

    factor.walk([&](Operation* op) {
        const std::optional<size_t> seeded = estimateSourceRows(op, metadata, estimation);
        if (seeded) {
            countedASource = true;
            rows = multiplySaturating(rows, *seeded);
            return;
        }

        const std::optional<RowMultiplier> multiplier = estimateRowMultiplier(op, estimation);
        if (multiplier) {
            rows = multiplySaturating(rows, multiplier->_numerator) / multiplier->_denominator;
        }
    });

    return countedASource ? rows : uncountableRows;
}

// Which way round the two sides go into the join, whose right factor is the built one. The
// built side is buffered whole and indexed while the probed side streams a chunk at a
// time, so the side to build is the one making the fewer rows.
bool buildsTheLeftFactor(const EqualityCross& match, const DBPassContext& context) {
    CrossProduct product = match._product;
    Region& leftFactor = product.getLeftFactor();
    Region& rightFactor = product.getRightFactor();

    // With no graph to count either side against, a factor holding a product of its own is
    // the one to probe - its rows multiply where a scan's are linear - and with neither or
    // both holding one the right factor is built, the way db.hash_join reads its regions.
    // The estimate needs no such proxy: it multiplies through the product itself.
    if (!context._view) {
        const bool leftHoldsAProduct = factorHoldsAProduct(leftFactor);
        const bool rightHoldsAProduct = factorHoldsAProduct(rightFactor);

        return rightHoldsAProduct && !leftHoldsAProduct;
    }

    const ::db::GraphView& view = *context._view;
    const ::db::GraphMetadata& metadata = view.metadata();
    const ::db::CardinalityEstimation estimation(view);

    return estimateFactorRows(leftFactor, metadata, estimation)
         < estimateFactorRows(rightFactor, metadata, estimation);
}

bool matchEqualityCross(FilterOp filter, EqualityCross& match) {
    EqOp equality = filter.getMask().getDefiningOp<EqOp>();
    if (!equality || !equality.getResult().hasOneUse()) {
        return false;
    }

    match._filter = filter;
    match._equality = equality;

    // Either operand may name either factor, so the two sides are told apart by the
    // factor each key's column belongs to rather than by the order they are written in.
    CrossProduct product;
    JoinKeySide first;
    JoinKeySide second;
    if (!matchKeySide(equality.getLhs(), first, product)
        || !matchKeySide(equality.getRhs(), second, product)) {
        return false;
    }

    if (product->getBlock() != filter->getBlock()) {
        return false;
    }

    match._product = product;

    const size_t leftCount = factorYieldColumns(product.getLeftFactor()).size();
    const auto yieldedByTheLeftFactor = [leftCount](const JoinKeySide& side) {
        return cast<OpResult>(side._column).getResultNumber() < leftCount;
    };

    // One key on each side is what makes this a join; two keys of one factor are a
    // single-variable predicate, which the filter applies where it already stands.
    if (yieldedByTheLeftFactor(first) == yieldedByTheLeftFactor(second)) {
        return false;
    }

    const bool firstIsOnTheLeft = yieldedByTheLeftFactor(first);
    match._left = firstIsOnTheLeft ? first : second;
    match._right = firstIsOnTheLeft ? second : first;
    match._left._factor = &product.getLeftFactor();
    match._right._factor = &product.getRightFactor();

    if (!keysShareAColumnType(match._left, match._right)) {
        return false;
    }

    const bool conesArePrivate = keyConeIsPrivateTo(match._left, equality, filter)
                                 && keyConeIsPrivateTo(match._right, equality, filter);
    if (!conesArePrivate) {
        return false;
    }

    return carriesJoinColumnsOnly(filter, match) && productRowsReachOnly(product, match);
}

// Sinks the ops rebuilding a side's key into its factor, so the key becomes a column the
// factor yields and the join can index it as that side is read. Answers where the key
// lands in the yield: the column's own place when the key is that column, a fresh last
// place when it is computed from it.
size_t sinkKeyIntoFactor(JoinKeySide& side, size_t factorFirstResult, mlir::OpBuilder& builder) {
    Yield yield = cast<Yield>(side._factor->front().getTerminator());
    const size_t columnIndex = cast<OpResult>(side._column).getResultNumber() - factorFirstResult;

    if (side._cone._ops.empty()) {
        return columnIndex;
    }

    mlir::IRMapping mapping;
    mapping.map(side._column, yield.getColumns()[columnIndex]);

    builder.setInsertionPoint(yield);
    for (Operation* const coneOp : side._cone._ops) {
        builder.clone(*coneOp, mapping);
    }

    const size_t keyColumn = yield.getNumOperands();
    yield->insertOperands(static_cast<unsigned>(keyColumn), mapping.lookup(side._key));

    return keyColumn;
}

void fuseHashJoin(EqualityCross& match, mlir::OpBuilder& builder) {
    CrossProduct product = match._product;

    Yield leftYield = cast<Yield>(product.getLeftFactor().front().getTerminator());
    Yield rightYield = cast<Yield>(product.getRightFactor().front().getTerminator());
    const size_t leftCount = leftYield.getNumOperands();
    const size_t rightCount = rightYield.getNumOperands();

    const size_t leftKey = sinkKeyIntoFactor(match._left, 0, builder);
    const size_t rightKey = sinkKeyIntoFactor(match._right, leftCount, builder);

    // The join probes its left factor and builds its right, so the two sides go in the
    // way round the match chose rather than the way round the product held them.
    const bool buildsTheLeftFactor = match._buildsTheLeftFactor;
    Yield probeYield = buildsTheLeftFactor ? rightYield : leftYield;
    Yield buildYield = buildsTheLeftFactor ? leftYield : rightYield;
    Region& probeFactor = buildsTheLeftFactor ? product.getRightFactor() : product.getLeftFactor();
    Region& buildFactor = buildsTheLeftFactor ? product.getLeftFactor() : product.getRightFactor();

    llvm::SmallVector<Type> resultTypes;
    llvm::append_range(resultTypes, probeYield.getColumns().getTypes());
    llvm::append_range(resultTypes, buildYield.getColumns().getTypes());

    const size_t probeKey = buildsTheLeftFactor ? rightKey : leftKey;
    const size_t buildKey = buildsTheLeftFactor ? leftKey : rightKey;

    builder.setInsertionPoint(product);
    HashJoin join = builder.create<HashJoin>(product.getLoc(), resultTypes, probeKey, buildKey);
    join.getLeftFactor().takeBody(probeFactor);
    join.getRightFactor().takeBody(buildFactor);

    // Sinking a key widens that factor's yield, so a column of the side the join reads
    // second sits further along in its results than it did in the product's.
    const size_t joinProbeCount = probeYield.getNumOperands();
    const size_t leftFirstResult = buildsTheLeftFactor ? joinProbeCount : 0;
    const size_t rightFirstResult = buildsTheLeftFactor ? 0 : joinProbeCount;

    llvm::SmallVector<Value> joinColumns;
    for (size_t columnIndex = 0; columnIndex < leftCount; columnIndex++) {
        joinColumns.push_back(join.getResult(leftFirstResult + columnIndex));
    }
    for (size_t columnIndex = 0; columnIndex < rightCount; columnIndex++) {
        joinColumns.push_back(join.getResult(rightFirstResult + columnIndex));
    }

    // A sunk key sits past the columns its factor already yielded, so it is none of those.
    const Value leftKeyColumn = join.getResult(leftFirstResult + leftKey);
    const Value rightKeyColumn = join.getResult(rightFirstResult + rightKey);

    // The join keeps only the rows the equality held on, so what the filter handed
    // downstream now comes from the join directly - a carried key from the column the
    // join yields for it, which is that key over the joined rows.
    const Operation::operand_range carried = match._filter.getColumnsToFilter();
    const ResultRange filtered = match._filter.getFilteredColumns();
    for (size_t columnIndex = 0; columnIndex < carried.size(); columnIndex++) {
        const Value carriedColumn = carried[columnIndex];

        if (carriedColumn == match._left._key) {
            filtered[columnIndex].replaceAllUsesWith(leftKeyColumn);
        } else if (carriedColumn == match._right._key) {
            filtered[columnIndex].replaceAllUsesWith(rightKeyColumn);
        } else {
            const unsigned productIndex = cast<OpResult>(carriedColumn).getResultNumber();
            filtered[columnIndex].replaceAllUsesWith(joinColumns[productIndex]);
        }
    }

    for (size_t columnIndex = 0; columnIndex < joinColumns.size(); columnIndex++) {
        product.getResult(columnIndex).replaceAllUsesWith(joinColumns[columnIndex]);
    }

    match._filter.erase();
    match._equality.erase();

    const JoinKeySide* const sides[] = {&match._left, &match._right};
    for (const JoinKeySide* const side : sides) {
        for (Operation* const coneOp : llvm::reverse(side->_cone._ops)) {
            eraseIfUnused(coneOp);
        }
    }

    product.erase();
}

struct FuseHashJoin : public impl::FuseHashJoinBase<FuseHashJoin> {
    FuseHashJoin() {}

    FuseHashJoin(const DBPassContext* context)
        : _context(context)
    {
    }

    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect the matches first: fusing erases ops, which would invalidate the walk.
        const DBPassContext& context = *_context;
        llvm::SmallVector<EqualityCross, 2> matches;
        root->walk([&matches, &context](FilterOp filter) {
            EqualityCross match;
            if (!matchEqualityCross(filter, match) || prefersTheProduct(match, context)) {
                return;
            }

            match._buildsTheLeftFactor = buildsTheLeftFactor(match, context);
            matches.push_back(match);
        });

        mlir::OpBuilder builder(&getContext());
        for (EqualityCross& match : matches) {
            fuseHashJoin(match, builder);
        }
    }

private:
    const DBPassContext* _context {&defaultPassContext};
};

// A whole node scan whose column reaches an equality filter through ops that only hand it
// on, the other side of the equality being the IDs to fetch. Each link is the result an op
// hands the column on as, from the one the equality reads back to the scan.
struct FetchNodesChain {
    FilterOp _filter {nullptr};
    EqOp _equality {nullptr};
    Value _nodes;
    Value _ids;
    Operation* _scan {nullptr};
    ArrayAttr _labels;
    llvm::SmallVector<OpResult> _links;
};

// The column a result is, as handed on by its op: a column carried through a filter, an
// unwind or a node fetch, or one a product's factor yields. Null for any other result.
Value handedOnFrom(OpResult result) {
    Operation* const op = result.getOwner();
    const size_t resultIndex = result.getResultNumber();

    if (isa<FilterOp>(op)) {
        return op->getOperand(resultIndex + 1);
    } else if (isa<Unwind, FetchNodes>(op)) {
        return resultIndex == 0 ? Value() : op->getOperand(resultIndex);
    } else if (CrossProduct product = dyn_cast<CrossProduct>(op)) {
        return productFactorColumn(product, resultIndex);
    }

    return {};
}

bool isFactorOfTheScanAlone(Block* block) {
    const bool inAProduct = isa_and_nonnull<CrossProduct>(block->getParentOp());

    return inAProduct
        && block->getOperations().size() == 2
        && cast<Yield>(block->getTerminator()).getNumOperands() == 1;
}

// A factor yielding the scanned column alone is left empty once the column is dropped,
// which only its product going away can account for - so the scan must be all it holds.
// A filter carrying the scanned column alone would be left a filter of nothing.
bool keepsAColumnOnceDropped(OpResult link, Value handedOn) {
    Operation* const op = link.getOwner();

    if (isa<CrossProduct>(op)) {
        Block* const factor = cast<Yield>(*handedOn.getUsers().begin())->getBlock();
        const bool yieldsOtherColumns = factor->getTerminator()->getNumOperands() > 1;
        const bool holdsTheScanAlone = isa_and_nonnull<ScanNodes, ScanNodesByLabel>(handedOn.getDefiningOp())
                                       && isFactorOfTheScanAlone(factor);

        return yieldsOtherColumns || holdsTheScanAlone;
    } else if (FilterOp filter = dyn_cast<FilterOp>(op)) {
        return filter.getColumnsToFilter().size() > 1;
    }

    return true;
}

bool matchFetchNodesChain(Value nodes, Value ids, FetchNodesChain& chain) {
    if (nodes == ids) {
        return false;
    }

    for (OpOperand& use : nodes.getUses()) {
        Operation* const user = use.getOwner();
        const bool readByTheEquality = user == chain._equality.getOperation();
        const bool carriedByTheFilter = user == chain._filter.getOperation() && use.getOperandNumber() > 0;

        if (!readByTheEquality && !carriedByTheFilter) {
            return false;
        }
    }

    chain._links.clear();

    Value column = nodes;
    while (!isa_and_nonnull<ScanNodes, ScanNodesByLabel>(column.getDefiningOp())) {
        const OpResult link = dyn_cast<OpResult>(column);
        if (!link) {
            return false;
        }

        const Value handedOn = handedOnFrom(link);
        if (!handedOn) {
            return false;
        }

        const bool readByTheLinkAlone = handedOn.hasOneUse();
        if (!readByTheLinkAlone || !keepsAColumnOnceDropped(link, handedOn)) {
            return false;
        }

        chain._links.push_back(link);
        column = handedOn;
    }

    Operation* const scan = column.getDefiningOp();
    Block* const scanBlock = scan->getBlock();

    const bool scanStandsBesideTheFilter = scanBlock == chain._filter->getBlock();
    if (!scanStandsBesideTheFilter && !isFactorOfTheScanAlone(scanBlock)) {
        return false;
    }

    chain._nodes = nodes;
    chain._ids = ids;
    chain._scan = scan;

    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(scan)) {
        chain._labels = scanByLabel.getLabels();
    }

    return true;
}

bool isEqualityOfTheFilter(EqOp equality, FilterOp filter) {
    if (!equality) {
        return false;
    }

    const bool readOnce = equality.getResult().hasOneUse();
    const bool besideTheFilter = equality->getBlock() == filter->getBlock();

    return readOnce && besideTheFilter;
}

bool matchFetchNodes(FilterOp filter, FetchNodesChain& chain) {
    EqOp equality = filter.getMask().getDefiningOp<EqOp>();
    if (!isEqualityOfTheFilter(equality, filter)) {
        return false;
    }

    chain._filter = filter;
    chain._equality = equality;

    const Value lhs = equality.getLhs();
    const Value rhs = equality.getRhs();

    return matchFetchNodesChain(lhs, rhs, chain) || matchFetchNodesChain(rhs, lhs, chain);
}

CrossProduct dropProductColumn(CrossProduct product, size_t resultIndex, mlir::RewriterBase& rewriter) {
    Operation* const productOp = product.getOperation();
    const size_t leftCount = factorYield(productOp, 0).getNumOperands();

    llvm::SmallBitVector keep(productOp->getNumResults(), true);
    keep.reset(resultIndex);

    llvm::SmallVector<Type> keptTypes;
    keptResultTypes(productOp, keep, keptTypes);

    rewriter.setInsertionPoint(product);
    CrossProduct trimmed = rewriter.create<CrossProduct>(product.getLoc(), keptTypes);

    replaceWithTrimmedFactors(productOp, trimmed.getOperation(), keep, leftCount);
    rewriter.eraseOp(productOp);

    return trimmed;
}

void dropCarriedColumn(Operation* op, size_t resultIndex, mlir::RewriterBase& rewriter) {
    CarrySetLayout layout;
    const bool carries = matchCarrySetLayout(op, layout);
    bioassert(carries, "A fetch chain link without a carry set");

    const size_t droppedIndex = resultIndex - layout._resultOffset;

    llvm::SmallVector<size_t> kept;
    for (size_t carriedIndex = 0; carriedIndex < carriedCount(op, layout); carriedIndex++) {
        if (carriedIndex != droppedIndex) {
            kept.push_back(carriedIndex);
        }
    }

    trimCarrySet(op, layout, kept, rewriter);
    rewriter.eraseOp(op);
}

// A product one of whose factors yields nothing makes the other factor's rows alone
void inlineTheFactorLeft(CrossProduct product, mlir::RewriterBase& rewriter) {
    Operation* const productOp = product.getOperation();
    const bool leftIsEmpty = factorYield(productOp, 0).getNumOperands() == 0;
    const bool rightIsEmpty = factorYield(productOp, 1).getNumOperands() == 0;

    if (!leftIsEmpty && !rightIsEmpty) {
        return;
    }

    Region& keptFactor = leftIsEmpty ? product.getRightFactor() : product.getLeftFactor();
    Block& keptBlock = keptFactor.front();
    Yield keptYield = cast<Yield>(keptBlock.getTerminator());

    const llvm::SmallVector<Value> yielded(keptYield.getColumns().begin(), keptYield.getColumns().end());

    product->getBlock()->getOperations().splice(Block::iterator(productOp),
                                                keptBlock.getOperations(),
                                                keptBlock.begin(),
                                                Block::iterator(keptYield));

    for (size_t index = 0; index < yielded.size(); index++) {
        product.getResult(index).replaceAllUsesWith(yielded[index]);
    }

    rewriter.eraseOp(product);
}

void fuseFetchNodes(FetchNodesChain& chain, mlir::RewriterBase& rewriter) {
    FilterOp filter = chain._filter;
    const Location loc = filter.getLoc();

    const Operation::operand_range carried = filter.getColumnsToFilter();
    const ResultRange filtered = filter.getFilteredColumns();

    llvm::SmallVector<Value> fetchCarried;
    llvm::SmallVector<Type> fetchTypes {chain._nodes.getType()};
    for (const Value column : carried) {
        if (column != chain._nodes) {
            fetchCarried.push_back(column);
            fetchTypes.push_back(column.getType());
        }
    }

    rewriter.setInsertionPoint(filter);
    FetchNodes fetch = rewriter.create<FetchNodes>(loc, fetchTypes, chain._ids, fetchCarried);

    llvm::SmallVector<Value> fetched(fetch->getResults().begin(), fetch->getResults().end());
    if (chain._labels) {
        keepLabelledRows(fetched, chain._labels, loc, rewriter);
    }

    size_t fetchedIndex = 1;
    for (size_t index = 0; index < carried.size(); index++) {
        const bool holdsTheNodes = carried[index] == chain._nodes;
        filtered[index].replaceAllUsesWith(holdsTheNodes ? fetched.front() : fetched[fetchedIndex++]);
    }

    rewriter.eraseOp(filter);
    rewriter.eraseOp(chain._equality);

    llvm::SmallVector<CrossProduct> products;
    for (const OpResult link : chain._links) {
        Operation* const op = link.getOwner();
        const size_t resultIndex = link.getResultNumber();

        if (CrossProduct product = dyn_cast<CrossProduct>(op)) {
            products.push_back(dropProductColumn(product, resultIndex, rewriter));
        } else {
            dropCarriedColumn(op, resultIndex, rewriter);
        }
    }

    rewriter.eraseOp(chain._scan);

    for (CrossProduct product : llvm::reverse(products)) {
        inlineTheFactorLeft(product, rewriter);
    }
}

struct FuseFetchNodes : public impl::FuseFetchNodesBase<FuseFetchNodes> {
    void runOnOperation() override {
        runFilterWorklist<FetchNodesChain>(getOperation(), matchFetchNodes, fuseFetchNodes);
    }
};

// A row-wise op a re-rooted pattern recomputes over its new rows rather than carrying
bool computesPerRow(Operation* op) {
    return isRowWiseOp(op) || isa<ListIndex, ToNullable, GetEdgeTypes>(op);
}

bool isHop(Operation* op) {
    return isa<GetOutEdges, GetInEdges, GetEdges>(op);
}

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

// Where a pattern's rows meet the rows of the column its node is equated to: a cross
// product with the pattern in one factor, or an unwind carrying the pattern's columns
struct SeedJunction {
    Operation* _op {nullptr};
    Region* _seedFactor {nullptr};
    llvm::SmallVector<Value> _patternOutputs;
    llvm::SmallVector<Value> _patternResults;
    llvm::SmallVector<Value> _seedResults;
};

bool standsFromTheJunction(Operation* op, const SeedJunction& junction) {
    if (!op || op->getBlock() != junction._op->getBlock()) {
        return false;
    }

    return !op->isBeforeInBlock(junction._op);
}

struct PatternSeed {
    SeedJunction _junction;
    SeedPattern _pattern;
    FilterOp _filter {nullptr};
    EqOp _equality {nullptr};
    Value _seeded;
    Value _ids;
};

// The value a pattern column was bound as, following the ops that only hand it on
Value patternOrigin(Value column, const SeedPattern& pattern) {
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

void collectPatternInputs(Value value, const SeedPattern& pattern, llvm::SmallPtrSetImpl<void*>& inputs) {
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
        if (originDef && pattern._ops.contains(originDef)) {
            inputs.insert(origin.getAsOpaquePointer());
        }
    }
}

Operation* findPatternRoot(Value column) {
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

void collectPatternOps(Operation* root, Operation* end, SeedPattern& pattern) {
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

// Checks every op of the pattern is one a re-rooted pattern can be rebuilt from, and
// records its steps and filters
bool analyzePattern(SeedPattern& pattern, llvm::ArrayRef<Operation*> orderedOps) {
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
            const bool walksFreely = !exploration.getEndNodes()
                                  && !exploration.getEndColumn()
                                  && !exploration.getEndsOnSeed()
                                  && !exploration.getDistinct()
                                  && exploration.getHopImports().empty();
            if (!walksFreely) {
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

// Whether the pattern can be walked out from the seeded node: every hop reached from one
// of its ends, a walk only ever from the node it starts at
bool isWalkableFrom(const SeedPattern& pattern, Value seeded) {
    llvm::SmallPtrSet<void*, 16> reached {seeded.getAsOpaquePointer()};
    llvm::SmallPtrSet<Operation*, 16> walked;

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

bool isPatternNode(const SeedPattern& pattern, Value origin) {
    if (origin == pattern._root->getResult(0)) {
        return true;
    }

    return llvm::any_of(pattern._steps, [origin](const PatternStep& step) { return step._to == origin; });
}

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

bool matchSeedJunction(OpResult seededResult, SeedJunction& junction, SeedPattern& pattern) {
    Operation* const op = seededResult.getOwner();
    junction._op = op;

    if (CrossProduct product = dyn_cast<CrossProduct>(op)) {
        const size_t leftCount = factorYield(op, 0).getNumOperands();
        const bool patternOnTheLeft = seededResult.getResultNumber() < leftCount;

        Region& patternFactor = patternOnTheLeft ? product.getLeftFactor() : product.getRightFactor();
        junction._seedFactor = patternOnTheLeft ? &product.getRightFactor() : &product.getLeftFactor();

        const Operation::operand_range patternYield = factorYieldColumns(patternFactor);
        junction._patternOutputs.assign(patternYield.begin(), patternYield.end());

        for (size_t index = 0; index < op->getNumResults(); index++) {
            const bool fromThePattern = (index < leftCount) == patternOnTheLeft;
            (fromThePattern ? junction._patternResults : junction._seedResults).push_back(op->getResult(index));
        }

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

    const Operation::operand_range patternOutputs = unwind.getColumnsToFilter();
    const ResultRange patternResults = unwind.getCarried();

    junction._seedResults.push_back(unwind.getElement());
    junction._patternOutputs.assign(patternOutputs.begin(), patternOutputs.end());
    junction._patternResults.assign(patternResults.begin(), patternResults.end());

    Operation* const root = findPatternRoot(patternOutputs[seededResult.getResultNumber() - 1]);
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

    // The unwound list is the seed's own, and the unwind carries the pattern alone
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

    // Nothing but the pattern and the unwind may read what the pattern binds
    for (Operation* const patternOp : orderedOps) {
        for (Operation* const user : patternOp->getUsers()) {
            if (user != op && !pattern._ops.contains(user)) {
                return false;
            }
        }
    }

    return analyzePattern(pattern, orderedOps);
}

// @param visited holds the values an earlier call reached without finding the seed, so a
// caller asking of every filter along a chain walks the chain once
bool readsTheSeed(Value value, const SeedJunction& junction, llvm::DenseSet<Value>& visited) {
    llvm::SmallVector<Value> worklist {value};

    while (!worklist.empty()) {
        const Value current = worklist.pop_back_val();
        if (!visited.insert(current).second) {
            continue;
        }

        if (llvm::is_contained(junction._seedResults, current)) {
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

bool isComputedFromTheSeed(Value ids, const SeedJunction& junction, Operation* filter) {
    llvm::SmallVector<Value> worklist {ids};
    llvm::DenseSet<Value> visited;
    llvm::DenseSet<Value> masksVisited;

    while (!worklist.empty()) {
        const Value current = worklist.pop_back_val();
        if (!visited.insert(current).second || llvm::is_contained(junction._seedResults, current)) {
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
bool readsTheJunctionRowWise(const SeedJunction& junction, FilterOp filter) {
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

bool matchPatternSeedSide(FilterOp filter, EqOp equality, Value seeded, Value ids, JunctionTraces& traces, PatternSeed& seed) {
    if (!isNodeColumn(seeded)) {
        return false;
    }

    Block* const block = filter->getBlock();
    const OpResult seededResult = traces.trace(seeded, block);
    if (!seededResult) {
        return false;
    }

    seed = PatternSeed {};
    if (!matchSeedJunction(seededResult, seed._junction, seed._pattern)) {
        return false;
    }

    const SeedJunction& junction = seed._junction;
    const size_t outputIndex = llvm::find(junction._patternResults, Value(seededResult)) - junction._patternResults.begin();
    seed._seeded = patternOrigin(junction._patternOutputs[outputIndex], seed._pattern);

    const bool walkableFromTheSeed = isPatternNode(seed._pattern, seed._seeded) && isWalkableFrom(seed._pattern, seed._seeded);
    if (!walkableFromTheSeed) {
        return false;
    }

    const bool idsOverTheSeedRows = isComputedFromTheSeed(ids, junction, filter) && readsTheJunctionRowWise(junction, filter);
    if (!idsOverTheSeedRows) {
        return false;
    }

    // A literal list equated to the root folds into a node-ID disjunction over the scan,
    // which fuse_scan_by_node_ids turns into a const scan listing the nodes at plan time
    UnwindEqualityCross unwindEquality;
    CrossProduct product = dyn_cast<CrossProduct>(junction._op);
    const bool seedsTheRoot = seed._seeded == seed._pattern._root->getResult(0);
    const bool foldsIntoAConstScan = product && seedsTheRoot && matchUnwindEqualityCross(product, unwindEquality);
    if (foldsIntoAConstScan) {
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

            if (_current.count(origin)) {
                materialized.map(pending, _current.lookup(origin));
            } else if (!def || !_pattern._ops.contains(def)) {
                materialized.map(pending, pending);
            } else {
                bioassert(computesPerRow(def), "A pattern column read before the step binding it");

                bool operandsPending = false;
                for (const Value operand : llvm::reverse(def->getOperands())) {
                    if (!materialized.contains(operand)) {
                        worklist.push_back(operand);
                        operandsPending = true;
                    }
                }

                if (operandsPending) {
                    continue;
                }

                Operation* const cloned = _builder.clone(*def, materialized);
                materialized.map(pending, cloned->getResult(cast<OpResult>(pending).getResultNumber()));
            }

            worklist.pop_back();
        }

        return materialized.lookup(value);
    }

    void walkFrom(Value seeded) {
        llvm::SmallPtrSet<Operation*, 16> walked;
        llvm::SmallPtrSet<Operation*, 16> applied;
        bool rootChecked = false;

        bool progressed = true;
        while (progressed) {
            progressed = false;

            if (!rootChecked && isAvailable(_pattern._root->getResult(0))) {
                applyRootLabels();
                rootChecked = true;
            }

            for (FilterOp filter : _pattern._filters) {
                const bool applicable = !applied.contains(filter.getOperation()) && dependsOnAvailable(filter.getMask());
                if (applicable) {
                    keepRows(materialize(filter.getMask()));
                    applied.insert(filter.getOperation());
                }
            }

            for (const PatternStep& step : _pattern._steps) {
                if (walked.contains(step._op)) {
                    continue;
                }

                if (isAvailable(step._from)) {
                    walkForward(step);
                } else if (isAvailable(step._to)) {
                    walkBackward(step);
                } else {
                    continue;
                }

                walked.insert(step._op);
                progressed = true;
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

    bool isAvailable(Value origin) const { return _current.count(origin) > 0; }

    bool dependsOnAvailable(Value value) const {
        llvm::SmallPtrSet<void*, 8> inputs;
        collectPatternInputs(value, _pattern, inputs);

        return llvm::all_of(inputs, [this](void* input) { return isAvailable(Value::getFromOpaquePointer(input)); });
    }

    void carried(llvm::SmallVectorImpl<Value>& columns, llvm::SmallVectorImpl<Type>& types) const {
        for (const Value origin : _inFlight) {
            const Value current = _current.lookup(origin);
            columns.push_back(current);
            types.push_back(current.getType());
        }
    }

    void rebindCarried(Operation* op, size_t firstCarried) {
        for (size_t index = 0; index < _inFlight.size(); index++) {
            _current[_inFlight[index]] = op->getResult(firstCarried + index);
        }
    }

    void keepRows(Value mask) {
        llvm::SmallVector<Value> columns;
        llvm::SmallVector<Type> types;
        carried(columns, types);

        FilterOp filter = _builder.create<FilterOp>(_loc, types, mask, columns);
        rebindCarried(filter.getOperation(), 0);
    }

    void applyRootLabels() {
        ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(_pattern._root);
        if (!scanByLabel) {
            return;
        }

        const Value root = _current.lookup(_pattern._root->getResult(0));
        keepRows(checkLabels(root, scanByLabel.getLabels(), _loc, _builder));
    }

    void bindHop(const PatternStep& step, Operation* walked, size_t fromResult, size_t toResult) {
        rebindCarried(walked, hopFixedResultCount);

        bind(step._from, walked->getResult(fromResult));
        bind(step._to, walked->getResult(toResult));
        bind(step._op->getResult(1), walked->getResult(1));
        bind(step._op->getResult(2), walked->getResult(2));
    }

    void hopTypes(Operation* hop, llvm::SmallVectorImpl<Type>& types) const {
        for (size_t index = 0; index < hopFixedResultCount; index++) {
            types.push_back(hop->getResult(index).getType());
        }
    }

    void walkForward(const PatternStep& step) {
        if (ExplorePaths exploration = dyn_cast<ExplorePaths>(step._op)) {
            walkExploration(exploration, step);
            return;
        }

        llvm::SmallVector<Value> columns;
        llvm::SmallVector<Type> types;
        hopTypes(step._op, types);
        carried(columns, types);

        OperationState state(_loc, step._op->getName());
        state.addOperands(_current.lookup(step._from));
        state.addOperands(columns);
        state.addTypes(types);
        Operation* const walked = _builder.create(state);

        bindHop(step, walked, hopInputResult(step._op), hopReachedResult(step._op));
    }

    // An out-hop reversed is an in-hop from the node it reached, over the same edges
    void walkBackward(const PatternStep& step) {
        llvm::SmallVector<Value> columns;
        llvm::SmallVector<Type> types;
        hopTypes(step._op, types);
        carried(columns, types);

        const Value from = _current.lookup(step._to);
        Operation* walked = nullptr;
        if (isa<GetOutEdges>(step._op)) {
            walked = _builder.create<GetInEdges>(_loc, types, from, columns).getOperation();
        } else if (isa<GetInEdges>(step._op)) {
            walked = _builder.create<GetOutEdges>(_loc, types, from, columns).getOperation();
        } else {
            walked = _builder.create<GetEdges>(_loc, types, from, columns).getOperation();
        }

        bindHop(step, walked, hopReachedResult(walked), hopInputResult(walked));
    }

    void walkExploration(ExplorePaths exploration, const PatternStep& step) {
        llvm::SmallVector<Value> columns;
        llvm::SmallVector<Type> types {exploration.getSrcids().getType(), exploration.getTgtids().getType(), exploration.getPaths().getType()};
        carried(columns, types);

        ExplorePaths walked = _builder.create<ExplorePaths>(_loc,
                                                            types,
                                                            _current.lookup(step._from),
                                                            columns,
                                                            Value(),
                                                            ValueRange(),
                                                            exploration.getDirection(),
                                                            exploration.getMinHops(),
                                                            exploration.getMaxHopsAttr(),
                                                            exploration.getEdgeTypesAttr(),
                                                            exploration.getEndLabelsAttr(),
                                                            exploration.getHopLabelsAttr(),
                                                            IntegerAttr(),
                                                            false,
                                                            exploration.getDistinct());
        walked.getHop().takeBody(exploration.getHop());

        rebindCarried(walked.getOperation(), pathFixedResultCount);
        bind(step._from, walked.getSrcids());
        bind(step._to, walked.getTgtids());
        bind(exploration.getPaths(), walked.getPaths());
    }
};

// Drops the equality from the filter's mask: the rebuilt rows hold it on every row
void dropSeedEquality(FilterOp filter, EqOp equality, mlir::RewriterBase& rewriter) {
    const Value mask = filter.getMask();
    const Value equal = equality.getResult();

    if (mask == equal) {
        bypassFilter(filter);
        rewriter.eraseOp(filter);
    } else {
        AndOp conjunction = cast<AndOp>(*equal.getUsers().begin());
        const Value other = conjunction.getLhs() == equal ? conjunction.getRhs() : conjunction.getLhs();

        conjunction.getResult().replaceAllUsesWith(other);
        rewriter.eraseOp(conjunction);
    }

    llvm::SmallVector<Operation*> dead {equality.getOperation()};
    llvm::SmallPtrSet<Operation*, 8> erased;
    while (!dead.empty()) {
        Operation* const op = dead.pop_back_val();
        if (erased.contains(op) || !op->use_empty()) {
            continue;
        }

        llvm::SmallVector<Operation*> operandDefs;
        for (const Value operand : op->getOperands()) {
            Operation* const def = operand.getDefiningOp();
            if (def && computesPerRow(def)) {
                operandDefs.push_back(def);
            }
        }

        erased.insert(op);
        rewriter.eraseOp(op);
        dead.append(operandDefs.begin(), operandDefs.end());
    }
}

Value cloneSeedIDs(Value ids, const SeedJunction& junction, mlir::IRMapping& seedColumns, mlir::OpBuilder& builder) {
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

void reroot(PatternSeed& seed, mlir::RewriterBase& rewriter) {
    SeedJunction& junction = seed._junction;
    Operation* const junctionOp = junction._op;
    const Location loc = junctionOp->getLoc();

    rewriter.setInsertionPoint(junctionOp);

    // The seed's rows on their own: the other factor's body, or the list unwound again
    // without the pattern's columns
    llvm::SmallVector<Value> seedColumns;
    if (junction._seedFactor) {
        const Operation::operand_range yielded = factorYieldColumns(*junction._seedFactor);
        seedColumns.assign(yielded.begin(), yielded.end());

        Block& seedBlock = junction._seedFactor->front();
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
        seedMapping.map(junction._seedResults[index], seedColumns[index]);
    }

    const Value ids = cloneSeedIDs(seed._ids, junction, seedMapping, rewriter);

    // A scan only produces nodes a fetch would keep, so its column is the seeded node as is
    llvm::SmallVector<Value> seededColumns {ids};
    const bool namesScannedNodes = isa_and_nonnull<ScanNodes, ScanNodesByLabel, ScanNodesByPropertyValue>(ids.getDefiningOp());
    if (namesScannedNodes) {
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
        replay.bind(junction._seedResults[index], seededColumns[index + 1]);
    }

    replay.bind(seed._seeded, seededColumns.front());
    replay.walkFrom(seed._seeded);

    for (size_t index = 0; index < junction._patternResults.size(); index++) {
        junction._patternResults[index].replaceAllUsesWith(replay.materialize(junction._patternOutputs[index]));
    }

    for (Value seedResult : junction._seedResults) {
        seedResult.replaceAllUsesWith(replay.currentOf(seedResult));
    }

    dropSeedEquality(seed._filter, seed._equality, rewriter);

    if (junction._seedFactor) {
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

// The node scans a column's rows come off, in factor order. A cross product's rows are its
// two factors' together, and a factor is free to be a product of its own, so each is walked
// through the column it yields first. Anything else a column can come off - a filter, a hop,
// a property read - has a row count only the engine can reach, so the match fails.
bool collectScans(Value column, llvm::SmallVectorImpl<Operation*>& scans) {
    llvm::SmallVector<Value> pending {column};
    while (!pending.empty()) {
        Operation* const def = pending.pop_back_val().getDefiningOp();
        if (!def) {
            return false;
        }

        if (isa<ScanNodes, ScanNodesByLabel>(def)) {
            scans.push_back(def);
        } else if (CrossProduct product = dyn_cast<CrossProduct>(def)) {
            const Operation::operand_range leftColumns = factorYieldColumns(product.getLeftFactor());
            const Operation::operand_range rightColumns = factorYieldColumns(product.getRightFactor());
            if (leftColumns.empty() || rightColumns.empty()) {
                return false;
            }

            pending.push_back(rightColumns.front());
            pending.push_back(leftColumns.front());
        } else {
            return false;
        }
    }

    return true;
}

// The one scan a column carries the nodes of. Unlike collectScans this follows the result a
// product was read at: its results are the left factor's yielded columns then the right's,
// so an index past the left's count names a column of the right factor.
Operation* scanBehind(Value column) {
    for (;;) {
        Operation* const def = column.getDefiningOp();
        if (!def) {
            return nullptr;
        }

        if (isa<ScanNodes, ScanNodesByLabel>(def)) {
            return def;
        }

        CrossProduct product = dyn_cast<CrossProduct>(def);
        if (!product) {
            return nullptr;
        }

        const Operation::operand_range leftColumns = factorYieldColumns(product.getLeftFactor());
        const Operation::operand_range rightColumns = factorYieldColumns(product.getRightFactor());
        const size_t resultIndex = cast<OpResult>(column).getResultNumber();
        if (resultIndex >= leftColumns.size() + rightColumns.size()) {
            return nullptr;
        }

        column = resultIndex < leftColumns.size() ? leftColumns[resultIndex]
                                                  : rightColumns[resultIndex - leftColumns.size()];
    }
}

// What a count reads: the scans it tallies, the property whose holders one of them is
// narrowed to, if any, and which scan that is.
struct ScanTally {
    llvm::SmallVector<Operation*> scans;
    StringAttr property;
    size_t propertyScan {0};
};

// The label conjunction db.count_scan_rows spells for one scan: its own label list, or an
// empty one for an unlabelled scan.
Attribute scanLabels(Operation* scan, mlir::OpBuilder& builder) {
    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(scan)) {
        return scanByLabel.getLabelsAttr();
    }

    return builder.getStrArrayAttr({});
}

// Whether this count tallies nothing but whole node scans, and what it reads off them.
// count(DISTINCT x) is a different tally over the same rows, so it is left alone.
bool countsWholeScans(Count count, ScanTally& tally) {
    if (count.getDistinct()) {
        return false;
    }

    Value rows = count.getInput();

    // A property read keeps every row and nulls the ones without the value, so count(*)
    // reads through it to the nodes underneath while a plain count tallies the holders.
    if (GetNodeProperties fetch = rows.getDefiningOp<GetNodeProperties>()) {
        rows = fetch.getInputNodes();
        if (!count.getRows()) {
            tally.property = fetch.getPropertyAttr();
        }
    } else {
        // A plain count over anything else charges the non-null rows, which is the row
        // count only on a column that holds no null - the node IDs a scan emits.
        const ColumnType rowsType = dyn_cast<ColumnType>(rows.getType());
        if (!count.getRows() && !(rowsType && isa<storage::NodeIDType>(rowsType.getType()))) {
            return false;
        }
    }

    if (!collectScans(rows, tally.scans)) {
        return false;
    }

    if (!tally.property) {
        return true;
    }

    // The property was read of one of the scans' columns; that scan is the one its holders
    // narrow, and the others contribute their whole node count.
    Operation* const propertyScan = scanBehind(rows);
    const auto scanIt = std::ranges::find(tally.scans, propertyScan);
    if (scanIt == tally.scans.end()) {
        return false;
    }

    tally.propertyScan = static_cast<size_t>(scanIt - tally.scans.begin());

    return true;
}

void countFromMetadata(Count count, const ScanTally& tally, mlir::OpBuilder& builder) {
    llvm::SmallVector<Attribute> labels;
    for (Operation* const scan : tally.scans) {
        labels.push_back(scanLabels(scan, builder));
    }

    // The first scan is where a property is read from unless the op says otherwise, so the
    // common single-scan form carries no index.
    const mlir::Type indexType = builder.getIntegerType(64, /*isSigned=*/false);
    const IntegerAttr propertyScan = tally.propertyScan > 0 ? IntegerAttr::get(indexType, tally.propertyScan)
                                                            : IntegerAttr();

    builder.setInsertionPoint(count);
    CountScanRows scanRows = builder.create<CountScanRows>(count.getLoc(),
                                                           count.getResult().getType(),
                                                           builder.getArrayAttr(labels),
                                                           tally.property,
                                                           propertyScan);

    Operation* const countOp = count.getOperation();
    const Value countedRows = count.getInput();

    countOp->getResult(0).replaceAllUsesWith(scanRows.getResult());
    countOp->erase();

    // Drop the now-dead chain, consumer to producer, each only if unused; a cross product
    // takes the scans in its factor regions down with it.
    Operation* const countedOp = countedRows.getDefiningOp();
    GetNodeProperties fetch = dyn_cast<GetNodeProperties>(countedOp);
    const Value scanned = fetch ? fetch.getInputNodes() : Value();

    eraseIfUnused(countedOp);
    if (scanned) {
        eraseIfUnused(scanned.getDefiningOp());
    }
}

// Whether the query writes to the graph. The graph's counts leave out the query's own
// writes, so none of its counts can be read off them.
bool writesTheGraph(Operation* root) {
    const WalkResult walked = root->walk([](Operation* op) {
        if (isa<CreateNode, CreateEdge, Merge, SetNodeProperty, SetEdgeProperty, DeleteNode, DeleteEdge>(op)) {
            return WalkResult::interrupt();
        }

        return WalkResult::advance();
    });

    return walked.wasInterrupted();
}

struct CountFromMetadata : public impl::CountFromMetadataBase<CountFromMetadata> {
    CountFromMetadata() {}

    CountFromMetadata(const DBPassContext* context)
        : _context(context)
    {
    }

    void runOnOperation() override {
        if (_context->_hasPendingWrites || writesTheGraph(getOperation())) {
            return;
        }

        // Collect the counts first, since rewriting erases ops and would invalidate the walk.
        llvm::SmallVector<Count> counts;
        getOperation()->walk([&](Count count) {
            counts.push_back(count);
        });

        mlir::OpBuilder builder(&getContext());
        for (const Count count : counts) {
            ScanTally tally;
            if (!countsWholeScans(count, tally)) {
                continue;
            }

            countFromMetadata(count, tally, builder);
        }
    }

private:
    const DBPassContext* _context {&defaultPassContext};
};

}

std::unique_ptr<Pass> createFuseHashJoin(const DBPassContext* context) {
    return std::make_unique<FuseHashJoin>(context);
}

std::unique_ptr<Pass> createCountFromMetadata(const DBPassContext* context) {
    return std::make_unique<CountFromMetadata>(context);
}

}
