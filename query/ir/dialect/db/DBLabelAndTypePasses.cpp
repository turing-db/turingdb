#include "DBPasses.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Transforms/GreedyPatternRewriteDriver.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallVector.h"

#include "StorageAttributes.h"
#include "DBOps.h"
#include "DBFilterUtils.h"
#include "DBKnownLabels.h"
#include "DBPassUtils.h"
#include "DBPassWorklist.h"

namespace mlir::db {

#define GEN_PASS_DEF_FUSELABELPREDICATES
#define GEN_PASS_DEF_FUSEEDGETYPEPREDICATES
#define GEN_PASS_DEF_NARROWEDGETYPEREADS
#define GEN_PASS_DEF_REMOVEREDUNDANTLABELCHECKS
#include "DBPasses.h.inc"

namespace {

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

// A parameter names no edge type before the program runs: a union lists it beside the
// names, where an intersection over it has no answer
bool holdsParameter(ArrayAttr names) {
    return llvm::any_of(names, [](Attribute name) { return isa<storage::ParameterAttr>(name); });
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

    const bool intersectsAParameter = !isUnion && (holdsParameter(left.getEdgeTypes()) || holdsParameter(right.getEdgeTypes()));
    if (intersectsAParameter) {
        return;
    }

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
// leaves the walk to skip everything else, and the check goes. A check the read already
// guarantees is the case where the intersection changes nothing, so only the check goes.
struct TypeChecksOverReads {
    FilterOp _filter;
    llvm::SmallVector<Value, 2> _conjuncts;
    Operation* _narrowedRead {nullptr};
    ArrayAttr _narrowedTypes;
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

// Narrowing a read changes what every reader of it sees, so the one read a filter narrows is
// the one making every column it cuts, which nothing but the filter reads
Operation* narrowableReadOf(FilterOp filter) {
    constexpr size_t etypesResultIndex = 2;
    Operation* const source = filteredSource(filter);
    const bool readsByType = source && source->getNumResults() > etypesResultIndex
                             && byTypeReadOf(source->getResult(etypesResultIndex)) == source;

    return readsByType && readsOnlyIntoTheFilter(source, filter) ? source : nullptr;
}

bool matchTypeChecksOverReads(FilterOp filter, TypeChecksOverReads& matched) {
    Operation* const narrowable = narrowableReadOf(filter);
    ArrayAttr narrowedTypes = narrowable ? narrowable->getAttrOfType<ArrayAttr>("edge_types") : ArrayAttr();

    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    for (const Value conjunct : conjuncts) {
        CheckEdgeTypeConstraint check = conjunct.getDefiningOp<CheckEdgeTypeConstraint>();
        Operation* const read = check ? byTypeReadOf(check.getEdgeTypeIds()) : nullptr;
        if (!read) {
            continue;
        }

        const ArrayAttr readTypes = read == narrowable ? narrowedTypes : read->getAttrOfType<ArrayAttr>("edge_types");
        const ArrayAttr checkTypes = check.getEdgeTypes();

        const bool readIsWithinCheck = llvm::all_of(readTypes, [checkTypes](Attribute edgeType) {
            return llvm::is_contained(checkTypes, edgeType);
        });

        const bool holdsAParameter = holdsParameter(readTypes) || holdsParameter(checkTypes);
        if (holdsAParameter && !readIsWithinCheck) {
            continue;
        }

        // An empty intersection is a read that matches no edge, which an empty type set on the
        // read says outright: the loop is then unmatchable and walks nothing.
        // The intersection is a subset of the read's types, so a smaller one is a narrower read
        llvm::SmallVector<Attribute, 4> intersection;
        combineEdgeTypes(false, readTypes, checkTypes, intersection);

        if (intersection.size() < readTypes.size()) {
            const bool narrowsTheRead = read == narrowable && check.getResult().hasOneUse();
            if (!narrowsTheRead) {
                continue;
            }

            narrowedTypes = ArrayAttr::get(filter.getContext(), intersection);
            matched._narrowedRead = read;
            matched._narrowedTypes = narrowedTypes;
        }

        matched._conjuncts.push_back(conjunct);
    }

    matched._filter = filter;

    return !matched._conjuncts.empty();
}

void foldTypeChecksIntoReads(TypeChecksOverReads& matched, mlir::RewriterBase& rewriter) {
    Operation* const read = matched._narrowedRead;

    if (read) {
        const ArrayAttr narrowedTypes = matched._narrowedTypes;
        rewriter.modifyOpInPlace(read, [read, narrowedTypes]() {
            read->setAttr("edge_types", narrowedTypes);
        });
    }

    dropConjuncts(matched._filter, matched._conjuncts, rewriter);
}

struct NarrowEdgeTypeReads : public impl::NarrowEdgeTypeReadsBase<NarrowEdgeTypeReads> {
    void runOnOperation() override {
        runFilterWorklist<TypeChecksOverReads>(getOperation(), matchTypeChecksOverReads, foldTypeChecksIntoReads);
    }
};

// A label check over nodes already known to carry every label of one of its alternatives
// keeps every row.
struct RedundantLabelChecks {
    FilterOp _filter;
    llvm::SmallVector<Value, 2> _conjuncts;
};

bool isRedundantLabelCheck(Value conjunct) {
    CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet) {
        return false;
    }

    llvm::DenseSet<Attribute> knownLabels;
    collectKnownLabels(labelSet.getInputNodes(), knownLabels);

    const auto isKnown = [&knownLabels](Attribute label) {
        return knownLabels.contains(label);
    };

    const auto isGuaranteed = [&isKnown](Attribute alternative) {
        return llvm::all_of(cast<ArrayAttr>(alternative), isKnown);
    };

    return llvm::any_of(check.getAlternatives(), isGuaranteed);
}

bool matchRedundantLabelChecks(FilterOp filter, RedundantLabelChecks& redundant) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    redundant._filter = filter;
    llvm::copy_if(conjuncts, std::back_inserter(redundant._conjuncts), isRedundantLabelCheck);

    return !redundant._conjuncts.empty();
}

void removeRedundantLabelChecks(RedundantLabelChecks& redundant, mlir::RewriterBase& rewriter) {
    dropConjuncts(redundant._filter, redundant._conjuncts, rewriter);
}

struct RemoveRedundantLabelChecks : public impl::RemoveRedundantLabelChecksBase<RemoveRedundantLabelChecks> {
    void runOnOperation() override {
        runFilterWorklist<RedundantLabelChecks>(getOperation(), matchRedundantLabelChecks, removeRedundantLabelChecks);
    }
};

}

}
