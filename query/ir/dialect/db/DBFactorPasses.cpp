#include "DBPasses.h"

#include <memory>
#include <vector>

#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Transforms/RegionUtils.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SetVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "IRRaiseEffect.h"
#include "DBOps.h"
#include "DBCarriedColumns.h"
#include "DBCrossProducts.h"
#include "DBFilterUtils.h"
#include "DBPassUtils.h"
#include "DBPassWorklist.h"
#include "DBRowCone.h"

#include "BioAssert.h"

namespace mlir::db {

#define GEN_PASS_DEF_PLACEINFACTORS
#include "DBPasses.h.inc"

namespace {

void appendCone(const MaskCone& from, llvm::SmallPtrSetImpl<Operation*>& appended, MaskCone& cone) {
    for (Operation* const op : from._ops) {
        if (appended.insert(op).second) {
            cone._ops.push_back(op);
        }
    }

    cone._inputs.insert(from._inputs.begin(), from._inputs.end());
}

// A cone moved into a factor still reads its constants from above the product, so the ops
// computing one after the product move ahead of it
void mapConeIntoFactors(const MaskCone& cone, CrossProduct product, ConeCache& cache, mlir::IRMapping& mapping, mlir::RewriterBase& rewriter) {
    llvm::SmallVector<Value> constants;
    for (const Value input : cone._inputs) {
        const Value column = cache.climbFilters(input);
        if (column.getDefiningOp() == product.getOperation()) {
            const size_t resultIndex = cast<OpResult>(column).getResultNumber();
            mapping.map(input, productFactorColumn(product, resultIndex));
        } else {
            constants.push_back(input);
        }
    }

    hoistDefinitionsAhead(constants, product, rewriter);
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

CrossProduct stackedProduct(FilterOp filter) {
    const Operation::operand_range columns = filter.getColumnsToFilter();
    if (columns.empty()) {
        return nullptr;
    }

    CrossProduct product = columns.front().getDefiningOp<CrossProduct>();
    if (!product || product->getBlock() != filter->getBlock()) {
        return nullptr;
    }

    for (const Value column : columns) {
        if (column.getDefiningOp() != product.getOperation()) {
            return nullptr;
        }
    }

    return product;
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
        const bool madeInTheBlock = def && def->getBlock() == block && ::db::hasNoEffectButRaising(def);

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

void sinkUnwindIntoFactor(UnwindPlacement& placement, ConeCache& cache, mlir::RewriterBase& rewriter) {
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
    mapConeIntoFactors(placement._source, product, cache, mapping, rewriter);

    rewriter.setInsertionPoint(yield);
    cloneRowCone(placement._source._ops, mapping, rewriter);

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
    eraseUnusedRowCone(placement._source._ops, rewriter);
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
    cloneRowCone(placement._source._ops, mapping, rewriter);

    const Value source = mapping.lookupOrDefault(unwind.getSource());
    Unwind listUnwind = rewriter.create<Unwind>(loc, TypeRange {elementType}, source, ValueRange {});
    rewriter.create<Yield>(loc, ValueRange {listUnwind.getElement()});

    const ResultRange carriedRows = unwind.getCarried();
    for (size_t index = 0; index < carriedRows.size(); index++) {
        rewriter.replaceAllUsesWith(carriedRows[index], product.getResult(index));
    }

    rewriter.replaceAllUsesWith(unwoundElement, product.getResult(carriedRows.size()));

    rewriter.eraseOp(unwind);
    eraseUnusedRowCone(placement._source._ops, rewriter);
}

void placeUnwind(UnwindPlacement& placement, ConeCache& cache, mlir::RewriterBase& rewriter) {
    if (placement._product) {
        sinkUnwindIntoFactor(placement, cache, rewriter);
    } else {
        unwindBesideItsRows(placement, rewriter);
    }
}

CrossProduct matchProductFactor(Region& factor) {
    Block& block = factor.front();
    if (!llvm::hasNItems(block.getOperations(), 2)) {
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

bool matchProductCut(FilterOp filter, CrossProduct product, ConeCache& cache, ProductCut& cut) {
    cut._filter = filter;
    cut._product = product;

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

void cutFactor(CrossProduct product,
               bool leftFactor,
               llvm::ArrayRef<Value> conjuncts,
               const MaskCone& cone,
               Location loc,
               ConeCache& cache,
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
    mapConeIntoFactors(cone, product, cache, mapping, rewriter);

    rewriter.setInsertionPoint(yield);
    cloneRowCone(cone._ops, mapping, rewriter);

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

void cutFactors(ProductCut& cut, ConeCache& cache, mlir::RewriterBase& rewriter) {
    const Location loc = cut._filter.getLoc();

    cutFactor(cut._product, true, cut._leftConjuncts, cut._leftCone, loc, cache, rewriter);
    cutFactor(cut._product, false, cut._rightConjuncts, cut._rightCone, loc, cache, rewriter);
}

void keepUncutConjuncts(ProductCut& cut, mlir::RewriterBase& rewriter) {
    FilterOp filter = cut._filter;
    const Location loc = filter.getLoc();
    const Value mask = filter.getMask();

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
    eraseUnusedRowCone(cut._leftCone._ops, rewriter);
    eraseUnusedRowCone(cut._rightCone._ops, rewriter);
}

CrossProduct rotateProduct(CrossProduct product, const ProductRotation& rotation, mlir::RewriterBase& rewriter) {
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

    return rotated;
}

// A rotation is pushed through in the same rewrite: the filter then stands in the factor the
// rotation made, which is no bare product any more, so no other filter can rotate it back
void regroupProductCut(ProductCut& cut, ConeCache& cache, mlir::RewriterBase& rewriter) {
    const CrossProduct rotated = rotateProduct(cut._product, cut._rotation, rewriter);
    cache.clear();

    ProductCut rotatedCut;
    const bool pushes = matchProductCut(cut._filter, rotated, cache, rotatedCut) && !rotatedCut._rotation._rotates;
    bioassert(pushes, "A rotated product leaves the conjunct that rotated it on one factor");

    cutFactors(rotatedCut, cache, rewriter);
    keepUncutConjuncts(rotatedCut, rewriter);
}

// Moves into the factors of the product under @param bottom what each filter stacked over it
// reads of one factor, bottom first, in one walk up the stack. The filters are only erased once
// the walk ends, so the stack and the climbs the cache holds stay valid throughout
bool placeFilterStack(FilterOp bottom, ConeCache& cache, mlir::RewriterBase& rewriter) {
    const CrossProduct product = stackedProduct(bottom);
    if (!product) {
        return false;
    }

    std::vector<std::unique_ptr<ProductCut>> cuts;
    FilterStack stack(product);
    for (FilterOp filter = bottom; filter; filter = stack.getReader()) {
        const bool exclusive = stack.isExclusiveTo(filter);
        stack.climbTo(filter);

        if (!exclusive) {
            continue;
        }

        ProductCut& cut = *cuts.emplace_back(std::make_unique<ProductCut>());
        if (!matchProductCut(filter, product, cache, cut)) {
            cuts.pop_back();
        } else if (cut._rotation._rotates) {
            break;
        } else {
            cutFactors(cut, cache, rewriter);
        }
    }

    for (const std::unique_ptr<ProductCut>& cut : cuts) {
        if (!cut->_rotation._rotates) {
            keepUncutConjuncts(*cut, rewriter);
        }
    }

    if (!cuts.empty() && cuts.back()->_rotation._rotates) {
        regroupProductCut(*cuts.back(), cache, rewriter);
    }

    return !cuts.empty();
}

bool placeInAFactor(Operation* op, ConeCache& cache, mlir::RewriterBase& rewriter) {
    FilterOp filter = dyn_cast<FilterOp>(op);

    bool placed = false;
    UnwindPlacement placement;
    if (filter) {
        placed = placeFilterStack(filter, cache, rewriter);
    } else if (matchUnwindPlacement(cast<Unwind>(op), cache, placement)) {
        placeUnwind(placement, cache, rewriter);
        placed = true;
    }

    if (placed) {
        cache.clear();
    }

    return placed;
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

}

}
