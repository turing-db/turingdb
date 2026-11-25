#include "DBPasses.h"

#include <stdint.h>
#include <algorithm>
#include <optional>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Transforms/RegionUtils.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SetVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "IRConstantColumn.h"
#include "StorageAttributes.h"
#include "PathHopArguments.h"
#include "DBOps.h"
#include "DBCarriedColumns.h"
#include "DBCrossProducts.h"
#include "DBFilterUtils.h"
#include "DBKnownLabels.h"
#include "DBLineageClimbs.h"
#include "DBLookupPredicates.h"
#include "DBPassUtils.h"
#include "DBPassWorklist.h"
#include "DBRowCone.h"
#include "DBWrittenOrder.h"

namespace mlir::db {

#define GEN_PASS_DEF_FUSEEXPLOREENDCONSTRAINT
#define GEN_PASS_DEF_HOISTHOPCONJUNCTS
#define GEN_PASS_DEF_FUSEEXPLOREHOPLABELS
#define GEN_PASS_DEF_FUSEEXPLOREENDNODES
#define GEN_PASS_DEF_FUSEEXPLOREENDFACTOR
#define GEN_PASS_DEF_FUSEEXPLOREENDSET
#define GEN_PASS_DEF_FUSEEXPLORELISTPREDICATE
#define GEN_PASS_DEF_FUSEEXPLOREDISTINCTENDS
#include "DBPasses.h.inc"

namespace {

// A path exploration whose rows are then cut down to those ending on labelled nodes: the
// end constraint spelled the long way, since the walk itself can keep to those ends and
// never build the rows the filter goes on to drop.
struct EndConstrainedExploration {
    FilterOp _filter;
    ExplorePaths _exploration;
    llvm::SmallVector<Value, 2> _conjuncts;
    llvm::SmallVector<Attribute, 4> _labels;
};

// The labels @param conjunct asks of every end of @param exploration, or null
ArrayAttr endLabelsAsked(Value conjunct, ExplorePaths exploration) {
    CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
    GetNodeLabelSet labelSet = check ? check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>() : GetNodeLabelSet();
    if (!labelSet || labelSet.getInputNodes() != exploration.getTgtids()) {
        return ArrayAttr();
    }

    return check.getConjunction();
}

bool matchEndConstrainedExploration(FilterOp filter, EndConstrainedExploration& constrained) {
    // Every column the filter cuts has to be one the exploration bound, or the constrained
    // exploration has nothing of its own to hand back in its place. And nothing outside the
    // filter may read the exploration, or that reader would go on seeing the rows the end
    // labels turn away.
    ExplorePaths exploration = dyn_cast_or_null<ExplorePaths>(filteredSource(filter));
    if (!exploration || !readsOnlyIntoTheFilter(exploration, filter)) {
        return false;
    }

    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    for (const Value conjunct : conjuncts) {
        const ArrayAttr labels = endLabelsAsked(conjunct, exploration);
        if (!labels) {
            continue;
        }

        constrained._conjuncts.push_back(conjunct);
        for (const Attribute label : labels) {
            if (!llvm::is_contained(constrained._labels, label)) {
                constrained._labels.push_back(label);
            }
        }
    }

    constrained._filter = filter;
    constrained._exploration = exploration;

    return !constrained._conjuncts.empty();
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

void fuseExploreEndConstraint(EndConstrainedExploration& constrained, mlir::RewriterBase& rewriter) {
    ExplorePaths exploration = constrained._exploration;
    const ArrayAttr endLabels = mergedEndLabels(exploration, rewriter.getArrayAttr(constrained._labels), rewriter);

    rewriter.modifyOpInPlace(exploration, [&exploration, endLabels]() {
        exploration.setEndLabelsAttr(endLabels);
    });

    dropConjuncts(constrained._filter, constrained._conjuncts, rewriter);
}

struct FuseExploreEndConstraint : public impl::FuseExploreEndConstraintBase<FuseExploreEndConstraint> {
    void runOnOperation() override {
        runFilterWorklist<EndConstrainedExploration>(getOperation(), matchEndConstrainedExploration, fuseExploreEndConstraint);
    }
};

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

void setHopMask(Region& hop, Yield yield, llvm::ArrayRef<Value> residual, mlir::RewriterBase& rewriter) {
    if (residual.empty()) {
        rewriter.eraseBlock(&hop.front());
        return;
    }

    rewriter.setInsertionPoint(yield);
    const Type boolType = boolColumnType(rewriter.getContext());

    Value mask = residual.front();
    for (const Value conjunct : llvm::drop_begin(residual)) {
        mask = rewriter.create<AndOp>(yield.getLoc(), boolType, mask, conjunct).getResult();
    }

    rewriter.modifyOpInPlace(yield, [&yield, mask]() {
        yield->setOperand(0, mask);
    });
}

// The labels the hop region of a step asks of the hop's end node join the step's labels. The
// region keeps its other conjuncts, and goes when there is none left.
bool fuseStepHopLabels(ExplorePaths exploration, size_t step, llvm::SmallVectorImpl<Attribute>& labels, mlir::RewriterBase& rewriter) {
    Region& hop = exploration.getHops()[step];
    if (hop.empty()) {
        return false;
    }

    Block& block = hop.front();
    Yield yield = dyn_cast<Yield>(block.getTerminator());
    if (!yield || yield->getNumOperands() != 1) {
        return false;
    }

    llvm::SmallVector<Value> conjuncts;
    llvm::SmallVector<AndOp> joins;
    collectHopConjuncts(yield->getOperand(0), conjuncts, joins);

    const BlockArgument end = block.getArgument(hopEndArgument(step));

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
        return false;
    }

    for (CheckLabelConstraint check : checks) {
        for (const Attribute label : check.getConjunction()) {
            if (!llvm::is_contained(labels, label)) {
                labels.push_back(label);
            }
        }
    }

    setHopMask(hop, yield, residual, rewriter);
    if (residual.empty()) {
        return true;
    }

    for (AndOp join : joins) {
        rewriter.eraseOp(join);
    }

    for (CheckLabelConstraint check : checks) {
        Operation* const labelSet = check.getLabelsetIds().getDefiningOp();
        rewriter.eraseOp(check);
        eraseIfUnused(labelSet, rewriter);
    }

    return true;
}

void fuseHopLabels(ExplorePaths exploration, mlir::RewriterBase& rewriter) {
    const size_t stepCount = exploration.getStepCount();

    llvm::SmallVector<Attribute> stepLabels;
    bool fused = false;
    for (size_t step = 0; step < stepCount; step++) {
        llvm::SmallVector<Attribute> labels;
        if (const ArrayAttr current = exploration.getStepHopLabels(step)) {
            labels.append(current.begin(), current.end());
        }

        fused |= fuseStepHopLabels(exploration, step, labels, rewriter);
        stepLabels.push_back(rewriter.getArrayAttr(labels));
    }

    if (!fused) {
        return;
    }

    exploration.setHopLabelsAttr(rewriter.getArrayAttr(stepLabels));

    const bool hasPredicate = hasHopPredicate(exploration.getHops());
    if (!hasPredicate) {
        exploration.getHopImportsMutable().clear();
    }
}

struct FuseExploreHopLabels : public impl::FuseExploreHopLabelsBase<FuseExploreHopLabels> {
    void runOnOperation() override {
        mlir::IRRewriter rewriter(&getContext());
        getOperation()->walk([&rewriter](ExplorePaths exploration) {
            fuseHopLabels(exploration, rewriter);
        });
    }
};

// A path exploration whose rows are then cut down to those ending on the node a carried
// column already holds, or on the seed the walk left: the bound end spelled the long way,
// since the walk itself can head for that node and never build the rows the filter goes on
// to drop. An absent _endColumn is the seed, which no carried column names.
struct EndBoundExploration {
    FilterOp _filter;
    Value _conjunct;
    ExplorePaths _exploration;
    std::optional<uint64_t> _endColumn;
};

bool matchEndBoundConjunct(Value conjunct, ExplorePaths exploration, EndBoundExploration& bound) {
    EqOp equality = conjunct.getDefiningOp<EqOp>();
    if (!equality) {
        return false;
    }

    // One side is the end node column, the other the carried copy of the bound variable
    const Value ends = exploration.getTgtids();
    const Value carried = equality.getLhs() == ends ? equality.getRhs() : equality.getLhs();
    if (equality.getLhs() != ends && equality.getRhs() != ends) {
        return false;
    }

    const OpResult carriedResult = dyn_cast<OpResult>(carried);
    if (!carriedResult || carriedResult.getOwner() != exploration.getOperation() || !isNodeColumn(carried)) {
        return false;
    }

    // A walk the pattern brings back to where it started compares its end against its own
    // seed, which is a result of the exploration like a carried column but not one of them
    const size_t resultIndex = carriedResult.getResultNumber();
    const bool endsOnSeed = carried == exploration.getSrcids();
    if (!endsOnSeed && resultIndex < pathFixedResultCount) {
        return false;
    }

    bound._conjunct = conjunct;
    bound._endColumn = endsOnSeed ? std::nullopt : std::optional<uint64_t> {resultIndex - pathFixedResultCount};

    return true;
}

bool matchEndBoundExploration(FilterOp filter, EndBoundExploration& bound) {
    ExplorePaths exploration = dyn_cast_or_null<ExplorePaths>(filteredSource(filter));
    const bool alreadyBound = exploration && (exploration.getEndColumn() || exploration.getEndsOnSeed() || exploration.getEndNodes());
    if (!exploration || alreadyBound || !readsOnlyIntoTheFilter(exploration, filter)) {
        return false;
    }

    bound._filter = filter;
    bound._exploration = exploration;

    return matchAConjunct(filter, [exploration, &bound](Value conjunct) {
        return matchEndBoundConjunct(conjunct, exploration, bound);
    });
}

void fuseExploreEndNodes(EndBoundExploration& bound, mlir::RewriterBase& rewriter) {
    ExplorePaths exploration = bound._exploration;
    const std::optional<uint64_t> endColumn = bound._endColumn;

    rewriter.modifyOpInPlace(exploration, [&exploration, &rewriter, endColumn]() {
        if (endColumn) {
            exploration.setEndColumnAttr(unsignedAttribute(*endColumn, rewriter));
        } else {
            exploration.setEndsOnSeed(true);
        }
    });

    dropConjunct(bound._filter, bound._conjunct, rewriter);
}

struct FuseExploreEndNodes : public impl::FuseExploreEndNodesBase<FuseExploreEndNodes> {
    void runOnOperation() override {
        runFilterWorklist<EndBoundExploration>(getOperation(), matchEndBoundExploration, fuseExploreEndNodes);
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
    takeHopRegions(exploration, set);

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

// A conjunct a scan of the graph answers, over the node column it reads: the nodes holding a
// property value, the nodes listed by id, or the ends of a walk whose last edge is listed by
// id. Its last hop reaches those ends over the listed edges, and may reach them over others,
// so that conjunct still tests the rows.
struct NodeScanPredicate {
    Value _column;
    StringAttr _property;
    TypedAttr _value;
    Value _boundValue;
    EntityIDList _nodeIDs;
    EntityIDList _lastEdgeIDs;
    storage::PathDirection _lastHop {storage::PathDirection::Forward};
    bool _listsLastEdges {false};
};

bool matchNodeIDPredicate(Value conjunct, NodeScanPredicate& predicate) {
    const Value column = nodeDisjunctionColumn(conjunct);
    if (!column || !collectIDDisjunction(conjunct, column, predicate._nodeIDs)) {
        return false;
    }

    predicate._column = column;

    return true;
}

bool matchWalkLastEdgePredicate(Value conjunct, NodeScanPredicate& predicate) {
    const Value column = disjunctionColumn(conjunct);
    WalkEndEdge endEdge;
    if (!column || !matchWalkEndEdge(column, endEdge) || endEdge._first) {
        return false;
    }

    if (!collectIDDisjunction(conjunct, column, predicate._lastEdgeIDs)) {
        return false;
    }

    ExplorePaths walk = endEdge._walk;

    predicate._column = walk.getTgtids();
    predicate._lastHop = walk.getStepDirection(walk.getStepCount() - 1);
    predicate._listsLastEdges = true;

    return true;
}

bool matchNodeScanPredicate(Value conjunct, NodeScanPredicate& predicate) {
    EqOp equality = conjunct.getDefiningOp<EqOp>();
    PropertyEquality property;
    if (!equality || !matchPropertyEquality(equality, property)) {
        return matchNodeIDPredicate(conjunct, predicate) || matchWalkLastEdgePredicate(conjunct, predicate);
    }

    // A read of the write buffer sees nodes no scan of the graph does
    GetNodeProperties read = property._read;
    if (read.getPending() || read.getAllPending()) {
        return false;
    }

    predicate._column = read.getInputNodes();
    predicate._property = read.getPropertyAttr();
    predicate._value = property._value;
    predicate._boundValue = property._boundValue;

    return true;
}

// The labels every node of the column carries, as a by-label read asks for them, or null
ArrayAttr knownLabelsOf(Value column, mlir::OpBuilder& builder) {
    llvm::DenseSet<Attribute> known;
    collectKnownLabels(column, known);
    if (known.empty()) {
        return ArrayAttr();
    }

    const auto orderKey = [](Attribute label) {
        if (const StringAttr name = dyn_cast<StringAttr>(label)) {
            return std::pair(false, name.getValue());
        }

        return std::pair(true, cast<storage::ParameterAttr>(label).getName());
    };

    llvm::SmallVector<Attribute> labels(known.begin(), known.end());
    llvm::sort(labels, [&orderKey](Attribute lhs, Attribute rhs) {
        return orderKey(lhs) < orderKey(rhs);
    });

    return builder.getArrayAttr(labels);
}

Value buildNodeScan(const NodeScanPredicate& predicate, ArrayAttr labels, Location loc, mlir::OpBuilder& builder) {
    const Type nodeColumnType = predicate._column.getType();

    if (predicate._property) {
        ScanNodesByPropertyValue scan = builder.create<ScanNodesByPropertyValue>(loc,
                                                                                 nodeColumnType,
                                                                                 predicate._property,
                                                                                 predicate._value,
                                                                                 predicate._boundValue,
                                                                                 labels);
        computeBoundValuesAhead(scan);

        return scan.getResult();
    }

    llvm::SmallVector<Value> nodes;
    if (predicate._listsLastEdges) {
        nodes.push_back(scanListedEdgeEnds(predicate._lastEdgeIDs, predicate._lastHop, true, loc, builder));
    } else {
        ConstScanNodes listed = builder.create<ConstScanNodes>(loc,
                                                               nodeColumnType,
                                                               predicate._nodeIDs._literals,
                                                               predicate._nodeIDs._bound);
        computeBoundValuesAhead(listed);
        nodes.push_back(listed.getResult());
    }

    if (labels) {
        keepLabelledRows(nodes, labels, loc, builder);
    }

    return nodes.front();
}

// The nodes a column holds on every row that satisfies a scannable predicate some hops away:
// the predicate's scan, walked back over each hop that bound its node from the one before.
// _hops runs from the predicate's node back to _column, which no hop bound; with no hop the
// set is the predicate itself. Nothing in it reads a row, so it can be built ahead of them all.
struct NodeSetChain {
    NodeScanPredicate _predicate;
    llvm::SmallVector<Operation*> _hops;
    Value _column;
};

void deriveNodeSetChain(const NodeScanPredicate& predicate, LineageClimbs& climbs, NodeSetChain& chain) {
    chain._predicate = predicate;

    Value column = climbs.anchorOf(predicate._column);
    while (column) {
        Operation* const hop = column.getDefiningOp();
        const bool reachedByHop = hop && isEdgeHop(hop) && cast<OpResult>(column).getResultNumber() == hopReachedResult(hop);
        if (!reachedByHop) {
            break;
        }

        chain._hops.push_back(hop);
        column = climbs.anchorOf(hop->getOperand(0));
    }

    chain._column = column;
}

// Builds the set at the builder's insertion point. A hop reaches a node once per edge, so
// each node is walked back from once
Value buildNodeSetChain(const NodeSetChain& chain, Location loc, mlir::OpBuilder& builder) {
    const NodeScanPredicate& predicate = chain._predicate;
    Value nodes = buildNodeScan(predicate, knownLabelsOf(predicate._column, builder), loc, builder);

    for (size_t index = 0; index < chain._hops.size(); index++) {
        Operation* const hop = chain._hops[index];

        if (index > 0) {
            RemoveDuplicates distinct = builder.create<RemoveDuplicates>(loc, TypeRange {nodes.getType()}, ValueRange {nodes}, Value());
            nodes = distinct.getResult(0);
        }

        const ArrayAttr labels = knownLabelsOf(hop->getOperand(0), builder);
        Operation* const reversed = createReversedHop(hop, nodes, labels, ValueRange(), builder);
        nodes = reversed->getResult(hopReachedResult(reversed));
    }

    return nodes;
}

// A path exploration whose rows a filter then cuts to those whose end is, or reaches over a
// chain of hops, a node a scan of the graph can list: the walk can head for the ends that
// chain leads back to, and leave unwalked the prefixes that cannot reach one. With no hop
// between, the predicate holds of the end itself and leaves the filter.
struct ExploreEndSet {
    FilterOp _filter;
    ExplorePaths _exploration;
    Value _conjunct;
    NodeSetChain _chain;
};

// The first op of the function's body that writes the graph or holds a write, null if none
Operation* firstGraphWriteOf(mlir::func::FuncOp function) {
    for (Operation& op : function.getBody().front()) {
        const WalkResult walked = op.walk([](Operation* nested) {
            return isGraphWrite(nested) ? WalkResult::interrupt() : WalkResult::advance();
        });

        if (walked.wasInterrupted()) {
            return &op;
        }
    }

    return nullptr;
}

// The end set is built ahead of the whole function, so it leaves out what the query writes
// before the walk
bool writesAheadOf(Operation* firstWrite, Operation* op) {
    if (!firstWrite) {
        return false;
    }

    Operation* const holder = firstWrite->getBlock()->findAncestorOpInBlock(*op);

    return !holder || holder == firstWrite || firstWrite->isBeforeInBlock(holder);
}

bool matchExploreEndSet(FilterOp filter,
                        LineageClimbs& climbs,
                        const llvm::DenseMap<Operation*, Operation*>& firstWrites,
                        size_t maxChainHops,
                        ExploreEndSet& endSet) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    OpAnswers reaching;

    for (const Value conjunct : conjuncts) {
        NodeScanPredicate predicate;
        if (!matchNodeScanPredicate(conjunct, predicate)) {
            continue;
        }

        NodeSetChain chain;
        deriveNodeSetChain(predicate, climbs, chain);
        if (chain._hops.size() > maxChainHops) {
            continue;
        }

        ExplorePaths exploration = chain._column ? chain._column.getDefiningOp<ExplorePaths>() : nullptr;
        if (!exploration || chain._column != exploration.getTgtids()) {
            continue;
        }

        // Every row the walk emits has to reach the filter, or a reader of the rows it cut
        // would miss them
        const bool alreadyBound = exploration.getEndColumn() || exploration.getEndsOnSeed() || exploration.getEndNodes();
        const bool besideTheFilter = exploration->getBlock() == filter->getBlock();
        if (alreadyBound || !besideTheFilter) {
            continue;
        }

        const bool reachesTheFilter = answerOnce(reaching, exploration, [&exploration, &filter]() {
            return rowsReachTheFilter(exploration, filter, keepsTheSourceRows);
        });

        if (!reachesTheFilter) {
            continue;
        }

        Operation* const firstWrite = firstWrites.lookup(exploration->getParentOfType<mlir::func::FuncOp>());
        if (writesAheadOf(firstWrite, exploration)) {
            continue;
        }

        endSet = ExploreEndSet {._filter = filter, ._exploration = exploration, ._conjunct = conjunct, ._chain = chain};

        return true;
    }

    return false;
}

void fuseExploreEndSet(ExploreEndSet& endSet, mlir::RewriterBase& rewriter) {
    ExplorePaths exploration = endSet._exploration;
    const NodeSetChain& chain = endSet._chain;

    mlir::func::FuncOp function = exploration->getParentOfType<mlir::func::FuncOp>();
    rewriter.setInsertionPointToStart(&function.getBody().front());
    const Value set = buildNodeSetChain(chain, exploration.getLoc(), rewriter);

    const bool endsOnThePredicate = chain._hops.empty();
    rewriter.modifyOpInPlace(exploration, [&exploration, set, endsOnThePredicate]() {
        exploration.getEndNodesMutable().assign(set);
        if (endsOnThePredicate) {
            exploration.removeEndLabelsAttr();
        }
    });

    const bool holdsTheConjunct = endsOnThePredicate && !chain._predicate._listsLastEdges;
    if (holdsTheConjunct && endSet._conjunct.hasOneUse()) {
        dropConjunct(endSet._filter, endSet._conjunct, rewriter);
    }
}

struct FuseExploreEndSet : public impl::FuseExploreEndSetBase<FuseExploreEndSet> {
    void runOnOperation() override {
        llvm::DenseMap<Operation*, Operation*> firstWrites;
        getOperation()->walk([&firstWrites](mlir::func::FuncOp function) {
            firstWrites[function.getOperation()] = firstGraphWriteOf(function);
        });

        LineageClimbs climbs;

        const auto rewrite = [&climbs](ExploreEndSet& endSet, mlir::RewriterBase& rewriter) {
            fuseExploreEndSet(endSet, rewriter);
            climbs.clear();
        };

        // A walk takes one end set, so every predicate on an end itself, the exact set, is
        // fused before any set reached back over hops can take its walk
        for (const size_t maxChainHops : {size_t(0), SIZE_MAX}) {
            const auto match = [&climbs, &firstWrites, maxChainHops](FilterOp filter, ExploreEndSet& endSet) {
                return matchExploreEndSet(filter, climbs, firstWrites, maxChainHops, endSet);
            };

            runFilterWorklist<ExploreEndSet>(getOperation(), match, rewrite);
        }
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

bool keepsTheWalkedRows(Operation& op) {
    return isa<FilterOp, ExpandPath, ListPredicate>(op) || computesPerRow(&op);
}

// Whether the body reads only the element, constants and carried columns holding one value per
// seed. Each carried argument the body reads gets the exploration operand to import, the rest null.
bool matchHopTest(ListPredicate predicate, ExplorePaths exploration, llvm::SmallVectorImpl<Value>& imports) {
    Block& body = predicate.getBody().front();
    const BlockArgument tag = body.getArgument(1);
    ComprehensionYield yield = cast<ComprehensionYield>(body.getTerminator());

    const auto readsOnlyTheHop = [&body, tag](Value operand) {
        const BlockArgument argument = dyn_cast<BlockArgument>(operand);
        if (argument && argument.getOwner() == &body) {
            return argument != tag;
        } else if (!argument && operand.getDefiningOp()->getBlock() == &body) {
            return true;
        }

        return ::db::yieldsConstantColumn(operand);
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

bool matchExploreListConjunct(FilterOp filter, Value conjunct, OpAnswers& reaching, ExploreListPredicate& match) {
    ListPredicate predicate = conjunct.getDefiningOp<ListPredicate>();
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

    const std::optional<uint64_t> expandedSteps = expansion.getSteps();
    if (expandedSteps && *expandedSteps != exploration.getStepCount()) {
        return false;
    }

    const bool reachesTheFilter = exploration->getBlock() == filter->getBlock()
                               && answerOnce(reaching, exploration, [&exploration, &filter]() {
                                      return rowsReachTheFilter(exploration, filter, keepsTheWalkedRows);
                                  });
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

bool matchExploreListPredicate(FilterOp filter, ExploreListPredicate& match) {
    OpAnswers reaching;

    return matchAConjunct(filter, [&filter, &reaching, &match](Value conjunct) {
        return matchExploreListConjunct(filter, conjunct, reaching, match);
    });
}

// The argument of a step's hop region holding the element a list of the expansion kind reads
// off that hop: the region opens on the step's earlier nodes and edges
unsigned hopArgumentOf(storage::PathExpansionKind expansionKind, size_t step) {
    switch (expansionKind) {
        case storage::PathExpansionKind::Sources:
            return hopSourceArgument(step);
        break;
        case storage::PathExpansionKind::Edges:
            return hopEdgeArgument(step);
        break;
        case storage::PathExpansionKind::Ends:
        case storage::PathExpansionKind::Nodes:
            return hopEndArgument(step);
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

// The block of a step's hop region, created over the step's hop and the imports the
// exploration already reads when the step had no predicate
Block* hopBlockOf(ExplorePaths exploration, size_t step, mlir::RewriterBase& rewriter) {
    Region& hop = exploration.getHops()[step];
    if (!hop.empty()) {
        return &hop.front();
    }

    mlir::MLIRContext* context = rewriter.getContext();
    const Type nodeType = ColumnType::get(context, storage::NodeIDType::get(context));
    const Type edgeType = ColumnType::get(context, storage::EdgeIDType::get(context));

    llvm::SmallVector<Type> argumentTypes;
    buildHopArgumentTypes(step, nodeType, edgeType, exploration.getHopImports(), argumentTypes);

    const llvm::SmallVector<Location> argumentLocations(argumentTypes.size(), exploration.getLoc());

    const mlir::OpBuilder::InsertionGuard guard(rewriter);

    return rewriter.createBlock(&hop, hop.end(), argumentTypes, argumentLocations);
}

// The yield of a step's hop region, the rewriter left inserting ahead of it: null for a block
// that yields nothing yet, the rewriter then inserting at its end
Yield hopYieldOf(Block* hopBlock, mlir::RewriterBase& rewriter) {
    Yield hopYield = hopBlock->empty() ? Yield {} : dyn_cast<Yield>(hopBlock->getTerminator());
    if (hopYield) {
        rewriter.setInsertionPoint(hopYield);
    } else {
        rewriter.setInsertionPointToEnd(hopBlock);
    }

    return hopYield;
}

// One more test the step's hop must pass, and-ed into what its region yields
void joinHopTest(Yield hopYield, Value hopTest, Location loc, mlir::RewriterBase& rewriter) {
    if (hopYield) {
        const Type boolType = boolColumnType(rewriter.getContext());
        const Value joined = rewriter.create<AndOp>(loc, boolType, hopYield.getColumns().front(), hopTest).getResult();

        rewriter.modifyOpInPlace(hopYield, [&hopYield, joined]() {
            hopYield->setOperand(0, joined);
        });
    } else {
        rewriter.create<Yield>(loc, ValueRange {hopTest});
    }
}

void addHopTest(const ExploreListPredicate& match, size_t step, mlir::RewriterBase& rewriter) {
    ExplorePaths exploration = match._exploration;
    ListPredicate predicate = match._predicate;

    Block* hopBlock = hopBlockOf(exploration, step, rewriter);
    Region& hop = *hopBlock->getParent();

    llvm::SmallVector<Value> hopCarried;
    for (const Value import : match._imports) {
        hopCarried.push_back(import ? carryIntoRegion(hop, import, rewriter) : Value {});
    }

    ExpandPath expansion = match._expansion;
    const Value hopElement = hopBlock->getArgument(hopArgumentOf(expansion.getKind(), step));

    const Yield hopYield = hopYieldOf(hopBlock, rewriter);
    const Value hopTest = cloneElementTest(predicate, hopElement, hopCarried, rewriter);

    joinHopTest(hopYield, hopTest, predicate.getLoc(), rewriter);
}

// A list over one step of the body is tested on that step's hops alone, a list over every
// hop on the hops of every step
void fuseExploreListPredicate(ExploreListPredicate& match, mlir::RewriterBase& rewriter) {
    FilterOp filter = match._filter;
    ExplorePaths exploration = match._exploration;
    ListPredicate predicate = match._predicate;
    ExpandPath expansion = match._expansion;

    llvm::SetVector<Value> hopTestReads;
    mlir::getUsedValuesDefinedAbove(predicate.getBody(), hopTestReads);
    hoistDefinitionsAhead(hopTestReads.getArrayRef(), exploration, rewriter);

    const std::optional<uint64_t> expandedStep = expansion.getStep();
    const size_t stepCount = exploration.getStepCount();
    for (size_t step = 0; step < stepCount; step++) {
        if (!expandedStep || *expandedStep == step) {
            addHopTest(match, step, rewriter);
        }
    }

    if (expansion.getKind() != storage::PathExpansionKind::Nodes) {
        dropConjunct(filter, predicate.getResult(), rewriter);
    } else {
        // nodes() holds the seed, which no hop ends on, so the seed is still tested row by row
        const Operation::operand_range predicateCarried = predicate.getColumnsToFilter();
        const llvm::SmallVector<Value> rowCarried(predicateCarried.begin(), predicateCarried.end());

        rewriter.setInsertionPoint(predicate);
        const Value seedTest = cloneElementTest(predicate, expansion.getSrcids(), rowCarried, rewriter);

        rewriter.replaceAllUsesWith(predicate.getResult(), seedTest);
        eraseUnusedRowConeOf(predicate, rewriter);
    }
}

struct FuseExploreListPredicate : public impl::FuseExploreListPredicateBase<FuseExploreListPredicate> {
    void runOnOperation() override {
        runFilterWorklist<ExploreListPredicate>(getOperation(), matchExploreListPredicate, fuseExploreListPredicate);
    }
};

// The ops of a hop region computing a conjunct, in block order, and the latest step whose
// region takes every walk argument they read. Arguments past @param walkArguments are
// imports, which every step takes, and values defined outside the region are constants.
size_t collectConjunctSlice(Value conjunct, Block& block, size_t walkArguments, llvm::SmallVectorImpl<Operation*>& slice) {
    size_t step = 0;
    llvm::SmallPtrSet<Operation*, 16> visited;
    llvm::SmallVector<Value> pending {conjunct};

    while (!pending.empty()) {
        const Value value = pending.pop_back_val();

        if (const BlockArgument argument = dyn_cast<BlockArgument>(value)) {
            const bool readsTheWalk = argument.getOwner() == &block && argument.getArgNumber() < walkArguments;
            if (readsTheWalk) {
                step = std::max(step, firstStepTakingHopArgument(argument.getArgNumber()));
            }

            continue;
        }

        Operation* const op = value.getDefiningOp();
        const bool computedHere = op && op->getBlock() == &block;
        if (!computedHere || !visited.insert(op).second) {
            continue;
        }

        slice.push_back(op);
        op->walk([&pending](Operation* nested) {
            pending.append(nested->operand_begin(), nested->operand_end());
        });
    }

    std::ranges::sort(slice, [](Operation* left, Operation* right) {
        return left->isBeforeInBlock(right);
    });

    return step;
}

// Clones a conjunct of the last step's region into the region of @param step, which takes
// the same walk arguments up to its own end node and the same imports after them
void hoistConjunct(ExplorePaths exploration,
                   Block& lastBlock,
                   size_t lastWalkArguments,
                   Value conjunct,
                   llvm::ArrayRef<Operation*> slice,
                   size_t step,
                   mlir::RewriterBase& rewriter) {
    Block* const hopBlock = hopBlockOf(exploration, step, rewriter);
    const size_t walkArguments = hopWalkArgumentCount(step);

    mlir::IRMapping mapping;
    for (const BlockArgument argument : lastBlock.getArguments()) {
        const size_t argumentNumber = argument.getArgNumber();
        if (argumentNumber < walkArguments) {
            mapping.map(argument, hopBlock->getArgument(static_cast<unsigned>(argumentNumber)));
        } else if (argumentNumber >= lastWalkArguments) {
            const size_t importIndex = argumentNumber - lastWalkArguments;
            mapping.map(argument, hopBlock->getArgument(static_cast<unsigned>(walkArguments + importIndex)));
        }
    }

    const Yield hopYield = hopYieldOf(hopBlock, rewriter);
    for (Operation* const op : slice) {
        rewriter.clone(*op, mapping);
    }

    joinHopTest(hopYield, mapping.lookupOrDefault(conjunct), conjunct.getLoc(), rewriter);
}

// Codegen writes the hop predicate in the last step's region, which takes the whole
// repetition: each conjunct that an earlier step's arguments already cover moves there, so
// the walk drops a prefix as soon as it fails
void hoistHopConjuncts(ExplorePaths exploration, mlir::RewriterBase& rewriter) {
    const size_t lastStep = exploration.getStepCount() - 1;
    Region& lastHop = exploration.getHops()[lastStep];
    if (lastStep == 0 || lastHop.empty()) {
        return;
    }

    Block& lastBlock = lastHop.front();
    Yield yield = dyn_cast<Yield>(lastBlock.getTerminator());
    if (!yield || yield->getNumOperands() != 1) {
        return;
    }

    llvm::SmallVector<Value> conjuncts;
    llvm::SmallVector<AndOp> joins;
    collectHopConjuncts(yield->getOperand(0), conjuncts, joins);

    const size_t lastWalkArguments = hopWalkArgumentCount(lastStep);

    llvm::SmallVector<Value> residual;
    llvm::SmallVector<Operation*> slice;
    for (const Value conjunct : conjuncts) {
        slice.clear();
        const size_t step = collectConjunctSlice(conjunct, lastBlock, lastWalkArguments, slice);

        if (step == lastStep) {
            residual.push_back(conjunct);
        } else {
            hoistConjunct(exploration, lastBlock, lastWalkArguments, conjunct, slice, step, rewriter);
        }
    }

    if (residual.size() == conjuncts.size()) {
        return;
    }

    setHopMask(lastHop, yield, residual, rewriter);
    if (residual.empty()) {
        return;
    }

    llvm::SmallVector<Operation*> computed;
    for (Operation& op : lastBlock.without_terminator()) {
        computed.push_back(&op);
    }

    for (Operation* const op : llvm::reverse(computed)) {
        if (op->use_empty()) {
            rewriter.eraseOp(op);
        }
    }
}

struct HoistHopConjuncts : public impl::HoistHopConjunctsBase<HoistHopConjuncts> {
    void runOnOperation() override {
        mlir::IRRewriter rewriter(&getContext());
        getOperation()->walk([&rewriter](ExplorePaths exploration) {
            hoistHopConjuncts(exploration, rewriter);
        });
    }
};

bool aggregatesDistinctly(llvm::ArrayRef<int64_t> kinds) {
    for (const int64_t kind : kinds) {
        switch (static_cast<storage::GroupAggregateKind>(kind)) {
            case storage::GroupAggregateKind::Min:
            case storage::GroupAggregateKind::Max:
            case storage::GroupAggregateKind::CountDistinct:
            case storage::GroupAggregateKind::SumDistinct:
            case storage::GroupAggregateKind::AvgDistinct:
            case storage::GroupAggregateKind::StDevDistinct:
            case storage::GroupAggregateKind::StDevPDistinct:
            break;

            case storage::GroupAggregateKind::Count:
            case storage::GroupAggregateKind::Sum:
            case storage::GroupAggregateKind::Avg:
            case storage::GroupAggregateKind::CountRows:
            case storage::GroupAggregateKind::StDev:
            case storage::GroupAggregateKind::StDevP:
            case storage::GroupAggregateKind::First:
                return false;
            break;
        }
    }

    return true;
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

}

}
