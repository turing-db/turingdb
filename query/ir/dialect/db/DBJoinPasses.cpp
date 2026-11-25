#include "DBPasses.h"

#include <stdint.h>
#include <algorithm>
#include <memory>
#include <optional>

#include "mlir/IR/Builders.h"
#include "mlir/IR/Dominance.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallBitVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/Support/MathExtras.h"

#include "CardinalityEstimation.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "views/GraphView.h"

#include "IRConstantColumn.h"
#include "DBOps.h"
#include "DBCarriedColumns.h"
#include "DBCrossProducts.h"
#include "DBFilterUtils.h"
#include "DBPassUtils.h"
#include "DBPassWorklist.h"
#include "DBRowCone.h"
#include "DBRowEstimator.h"

#include "BioAssert.h"

namespace mlir::db {

#define GEN_PASS_DEF_FUSEHASHJOIN
#define GEN_PASS_DEF_FUSEFETCHNODES
#include "DBPasses.h.inc"

namespace {

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

// Reads into side the one product column a key is computed over, along with the ops
// computing one from the other. False when the key reads no column, more than one, or one
// a different cross product made - product names the one already matched, null for the
// first side, and is set to the one this side found.
bool matchKeySide(Value key, JoinKeySide& side, CrossProduct& product) {
    side._key = key;
    collectRowCone({key}, key.getParentBlock(), side._cone);

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
        if (isa<CrossProduct>(op)) {
            return WalkResult::interrupt();
        }

        return WalkResult::advance();
    });

    return walked.wasInterrupted();
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

// An op reading the rows of a cut, and how many of those rows the skips between them drop
struct CutRowsReader {
    Operation* _op {nullptr};
    uint64_t _skipped {0};
};

// The rows the cut must produce for a limit downstream of it to be met, and none when no
// limit bounds them. The budget reaches the cut only through ops handing each row on as at
// least one row as they read it, which the filter keeping other conjuncts than the equality
// does not. A skip adds the rows it drops to the limit after it, and a LIMIT 0 needs no row:
// the loops stop before their first step. A count bound when the query runs bounds nothing
// the plan can know.
std::optional<uint64_t> limitOverTheCut(const EqualityCross& match) {
    FilterOp filter = match._filter;
    EqOp equality = match._equality;
    if (filter.getMask() != equality.getResult()) {
        return std::nullopt;
    }

    Block* const cutBlock = filter->getBlock();

    llvm::SmallVector<CutRowsReader, 8> pending;
    llvm::SmallPtrSet<Operation*, 8> visited;
    const auto pushReaders = [&](Operation* op, uint64_t skipped) {
        for (Operation* const user : op->getUsers()) {
            Operation* const reader = cutBlock->findAncestorOpInBlock(*user);
            if (visited.insert(reader).second) {
                pending.push_back(CutRowsReader {reader, skipped});
            }
        }
    };

    pushReaders(filter, 0);

    std::optional<uint64_t> budget;
    while (!pending.empty()) {
        const CutRowsReader reader = pending.pop_back_val();
        Operation* const op = reader._op;

        if (Limit limit = dyn_cast<Limit>(op)) {
            const std::optional<uint64_t> count = limit.getCount();
            if (!count) {
                return std::nullopt;
            }

            const uint64_t rows = *count == 0 ? 0 : llvm::SaturatingAdd(reader._skipped, *count);
            budget = std::max(budget.value_or(0), rows);
        } else if (Skip skip = dyn_cast<Skip>(op)) {
            const std::optional<uint64_t> count = skip.getCount();
            if (!count) {
                return std::nullopt;
            }

            pushReaders(op, llvm::SaturatingAdd(reader._skipped, *count));
        } else if (computesAValuePerRow(op) || isa<OptionalMatch>(op)) {
            pushReaders(op, reader._skipped);
        } else {
            return std::nullopt;
        }
    }

    return budget;
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

    return estimation.shouldPreferCartesian(leftLabels, rightLabels, limitOverTheCut(match));
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

    const RowEstimator estimator(*context._view);

    return estimateFactorRows(leftFactor, estimator) < estimateFactorRows(rightFactor, estimator);
}

bool matchEqualityConjunct(FilterOp filter, Value conjunct, OpAnswers& readAlone, EqualityCross& match) {
    EqOp equality = conjunct.getDefiningOp<EqOp>();
    if (!equality || !equality.getResult().hasOneUse()) {
        return false;
    }

    match = EqualityCross {};
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

    if (!isHashableKey(match._left) || !isHashableKey(match._right)) {
        return false;
    }

    const bool conesArePrivate = keyConeIsPrivateTo(match._left, equality, filter)
                                 && keyConeIsPrivateTo(match._right, equality, filter);
    if (!conesArePrivate) {
        return false;
    }

    // The join emits the rows the equality holds on, so a reader of the product's own rows
    // would go from seeing every pair to seeing only the matching ones
    return carriesJoinColumnsOnly(filter, match) && answerOnce(readAlone, product, [&product, &filter]() {
        return readsOnlyIntoTheFilter(product, filter);
    });
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

void fuseHashJoin(EqualityCross& match, mlir::RewriterBase& rewriter) {
    CrossProduct product = match._product;

    Yield leftYield = cast<Yield>(product.getLeftFactor().front().getTerminator());
    Yield rightYield = cast<Yield>(product.getRightFactor().front().getTerminator());
    const size_t leftCount = leftYield.getNumOperands();
    const size_t rightCount = rightYield.getNumOperands();

    const size_t leftKey = sinkKeyIntoFactor(match._left, 0, rewriter);
    const size_t rightKey = sinkKeyIntoFactor(match._right, leftCount, rewriter);

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

    rewriter.setInsertionPoint(product);
    HashJoin join = rewriter.create<HashJoin>(product.getLoc(), resultTypes, probeKey, buildKey);
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

    // The join keeps only the rows the equality held on, so what the filter carries now
    // comes from the join directly - a carried key from the column the join yields for it,
    // which is that key over the joined rows.
    FilterOp filter = match._filter;
    const Operation::operand_range carried = filter.getColumnsToFilter();
    rewriter.modifyOpInPlace(filter, [&]() {
        for (size_t columnIndex = 0; columnIndex < carried.size(); columnIndex++) {
            const Value carriedColumn = carried[columnIndex];
            const unsigned operandIndex = static_cast<unsigned>(columnIndex) + 1;

            if (carriedColumn == match._left._key) {
                filter->setOperand(operandIndex, leftKeyColumn);
            } else if (carriedColumn == match._right._key) {
                filter->setOperand(operandIndex, rightKeyColumn);
            }
        }
    });

    for (size_t columnIndex = 0; columnIndex < joinColumns.size(); columnIndex++) {
        rewriter.replaceAllUsesWith(product.getResult(columnIndex), joinColumns[columnIndex]);
    }

    dropConjunct(filter, match._equality.getResult(), rewriter);
    rewriter.eraseOp(product);
}

struct FuseHashJoin : public impl::FuseHashJoinBase<FuseHashJoin> {
    FuseHashJoin() {}

    FuseHashJoin(const DBPassContext* context)
        : _context(context)
    {
    }

    void runOnOperation() override {
        const DBPassContext& context = *_context;

        const auto match = [&context](FilterOp filter, EqualityCross& cross) {
            OpAnswers readAlone;

            return matchAConjunct(filter, [&filter, &cross, &context, &readAlone](Value conjunct) {
                return matchEqualityConjunct(filter, conjunct, readAlone, cross) && !prefersTheProduct(cross, context);
            });
        };

        const auto fuse = [&context](EqualityCross& cross, mlir::RewriterBase& rewriter) {
            cross._buildsTheLeftFactor = buildsTheLeftFactor(cross, context);
            fuseHashJoin(cross, rewriter);
        };

        runFilterWorklist<EqualityCross>(getOperation(), match, fuse);
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
    llvm::SmallVector<Value, 2> _otherConjuncts;
    Value _nodes;
    Value _ids;
    Operation* _scan {nullptr};
    ArrayAttr _labels;
    llvm::SmallVector<OpResult> _links;
    llvm::SmallVector<std::pair<BroadcastConstant, Value>> _redrivenBroadcasts;
};

// A db.broadcast_constant reads no value of its driver, only how many rows it holds
bool readsTheRowsAlone(OpOperand& use) {
    BroadcastConstant broadcast = dyn_cast<BroadcastConstant>(use.getOwner());

    return broadcast && &use != &broadcast.getValueMutable();
}

// The one op reading the values of @param column, or null when there is not exactly one use
// reading them
Operation* soleValueReader(Value column) {
    Operation* reader = nullptr;
    for (OpOperand& use : column.getUses()) {
        if (readsTheRowsAlone(use)) {
            continue;
        } else if (reader) {
            return nullptr;
        }

        reader = use.getOwner();
    }

    return reader;
}

// A column @param reader hands on beside @param column, so one holding the same rows, or
// null when it hands on no other
Value siblingColumnOf(Value column, Operation* reader) {
    OperandRange candidates = reader->getOperands();

    CarrySet carrySet;
    if (matchCarrySet(reader, carrySet)) {
        candidates = candidates.slice(carrySet._operandOffset, carrySet._count);
    } else if (!isa<Yield>(reader)) {
        return {};
    }

    for (const Value candidate : candidates) {
        if (candidate != column) {
            return candidate;
        }
    }

    return {};
}

// The broadcasts counting the rows of @param column, which the fetch removes, each paired
// with the column it counts instead: one @param reader hands on beside it, else
// @param fallback, else none when the column was the only one holding those rows. False
// when that column is not bound ahead of a broadcast reading it.
bool collectRedrivenBroadcasts(Value column, Operation* reader, Value fallback, FetchNodesChain& chain) {
    const Value sibling = siblingColumnOf(column, reader);
    mlir::DominanceInfo dominance;

    for (OpOperand& use : column.getUses()) {
        if (!readsTheRowsAlone(use)) {
            continue;
        }

        BroadcastConstant broadcast = cast<BroadcastConstant>(use.getOwner());

        Value driver;
        if (sibling && dominance.properlyDominates(sibling, broadcast)) {
            driver = sibling;
        } else if (fallback && dominance.properlyDominates(fallback, broadcast)) {
            driver = fallback;
        } else if (sibling || fallback) {
            return false;
        }

        chain._redrivenBroadcasts.emplace_back(broadcast, driver);
    }

    return true;
}

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
        && llvm::hasNItems(block->getOperations(), 2)
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

        if (!readByTheEquality && !carriedByTheFilter && !readsTheRowsAlone(use)) {
            return false;
        }
    }

    chain._links.clear();
    chain._redrivenBroadcasts.clear();

    if (!collectRedrivenBroadcasts(nodes, chain._filter, ids, chain)) {
        return false;
    }

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

        Operation* const reader = soleValueReader(handedOn);
        if (!reader || !keepsAColumnOnceDropped(link, handedOn)) {
            return false;
        }

        if (!collectRedrivenBroadcasts(handedOn, reader, Value(), chain)) {
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

// Whether @param conjunct is computed by row computations from the columns @param filter
// carries alone, so it can be rebuilt over the rows a fusion makes in place of those columns
bool computesFromTheCarriedColumns(FilterOp filter, Value conjunct) {
    const Operation::operand_range carried = filter.getColumnsToFilter();
    Block* const block = filter->getBlock();

    llvm::SmallVector<Value> pending {conjunct};
    llvm::DenseSet<Value> visited;
    while (!pending.empty()) {
        const Value value = pending.pop_back_val();
        if (llvm::is_contained(carried, value) || !visited.insert(value).second) {
            continue;
        }

        Operation* const def = value.getDefiningOp();
        if (def && def->getBlock() != block && ::db::yieldsConstantColumn(value)) {
            continue;
        } else if (!def || def->getBlock() != block || !computesFromTheRow(def)) {
            return false;
        }

        llvm::append_range(pending, def->getOperands());
    }

    return true;
}

bool matchFetchNodesConjunct(FilterOp filter, Value conjunct, FetchNodesChain& chain) {
    EqOp equality = conjunct.getDefiningOp<EqOp>();
    if (!isEqualityOfTheFilter(equality, filter)) {
        return false;
    }

    chain._filter = filter;
    chain._equality = equality;

    const Value lhs = equality.getLhs();
    const Value rhs = equality.getRhs();

    return matchFetchNodesChain(lhs, rhs, chain) || matchFetchNodesChain(rhs, lhs, chain);
}

// The fetch takes one conjunct and rebuilds the others over the fetched rows, so at most one
// conjunct may be one it cannot rebuild, and that one has to be the one it takes
bool matchFetchNodes(FilterOp filter, FetchNodesChain& chain) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    Value unrebuilt;
    for (const Value conjunct : conjuncts) {
        if (computesFromTheCarriedColumns(filter, conjunct)) {
            continue;
        } else if (unrebuilt && unrebuilt != conjunct) {
            return false;
        }

        unrebuilt = conjunct;
    }

    for (const Value conjunct : conjuncts) {
        const bool rebuildsTheOthers = !unrebuilt || unrebuilt == conjunct;
        if (!rebuildsTheOthers || !matchFetchNodesConjunct(filter, conjunct, chain)) {
            continue;
        }

        chain._otherConjuncts.assign(conjuncts.begin(), conjuncts.end());
        llvm::erase(chain._otherConjuncts, conjunct);

        return true;
    }

    return false;
}

// Cuts @param columns, the rows a fusion makes in place of the columns @param filter carries,
// one for one, to the rows @param conjuncts keep, their computation rebuilt over them. Fills
// @param cone with the ops computing them over the filter's own rows.
void keepConjunctsOver(FilterOp filter,
                       llvm::ArrayRef<Value> conjuncts,
                       llvm::SmallVectorImpl<Value>& columns,
                       MaskCone& cone,
                       mlir::RewriterBase& rewriter) {
    if (conjuncts.empty()) {
        return;
    }

    Block* const block = filter->getBlock();
    const auto computedFromTheRows = [block](Operation* op) {
        return op->getBlock() == block && computesFromTheRow(op);
    };

    llvm::SmallPtrSet<Operation*, 8> visited;
    for (const Value conjunct : conjuncts) {
        collectConePostOrder(conjunct, computedFromTheRows, visited, cone);
    }

    mlir::IRMapping mapping;
    for (const auto [carried, column] : llvm::zip_equal(filter.getColumnsToFilter(), columns)) {
        mapping.map(carried, column);
    }

    cloneRowCone(cone._ops, mapping, rewriter);

    llvm::SmallVector<Value, 4> rebuilt;
    for (const Value conjunct : conjuncts) {
        rebuilt.push_back(mapping.lookupOrDefault(conjunct));
    }

    const ValueRange columnRange(columns);
    const Value mask = conjoin(rebuilt, filter.getLoc(), rewriter);
    FilterOp kept = rewriter.create<FilterOp>(filter.getLoc(), columnRange.getTypes(), mask, columnRange);

    columns.assign(kept.getResults().begin(), kept.getResults().end());
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
    CarrySet carrySet;
    const bool carries = matchCarrySet(op, carrySet);
    bioassert(carries, "A fetch chain link without a carry set");

    const size_t droppedIndex = resultIndex - carrySet._resultOffset;

    llvm::SmallVector<size_t> kept;
    for (size_t carriedIndex = 0; carriedIndex < carrySet._count; carriedIndex++) {
        if (carriedIndex != droppedIndex) {
            kept.push_back(carriedIndex);
        }
    }

    trimCarrySet(op, carrySet, kept, rewriter);
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
    for (const std::pair<BroadcastConstant, Value>& redriven : chain._redrivenBroadcasts) {
        BroadcastConstant broadcast = redriven.first;
        const Value driver = redriven.second;

        rewriter.modifyOpInPlace(broadcast, [&broadcast, driver] {
            mlir::MutableOperandRange driverOperand = broadcast.getDriverMutable();
            if (driver) {
                driverOperand.assign(driver);
            } else {
                driverOperand.clear();
            }
        });
    }

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

    llvm::SmallVector<Value> columns;
    size_t fetchedIndex = 1;
    for (const Value column : carried) {
        const bool holdsTheNodes = column == chain._nodes;
        columns.push_back(holdsTheNodes ? fetched.front() : fetched[fetchedIndex++]);
    }

    MaskCone otherConjunctsCone;
    keepConjunctsOver(filter, chain._otherConjuncts, columns, otherConjunctsCone, rewriter);

    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(columns[index]);
    }

    const Value mask = filter.getMask();
    rewriter.eraseOp(filter);
    eraseUnusedConjunction(mask, rewriter);
    rewriter.eraseOp(chain._equality);
    eraseUnusedRowCone(otherConjunctsCone._ops, rewriter);

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

}

std::unique_ptr<Pass> createFuseHashJoin(const DBPassContext* context) {
    return std::make_unique<FuseHashJoin>(context);
}

}
