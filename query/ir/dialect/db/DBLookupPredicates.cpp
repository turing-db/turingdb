#include "DBLookupPredicates.h"

#include <algorithm>

#include "mlir/IR/PatternMatch.h"
#include "mlir/Transforms/RegionUtils.h"

#include "llvm/ADT/STLExtras.h"

#include "EntityIDOfNumber.h"

#include "IRConstantColumn.h"
#include "DBColumnElement.h"
#include "PropertyScanLiteral.h"
#include "DBFilterUtils.h"
#include "DBPassUtils.h"

#include "BioAssert.h"

using namespace mlir;
using namespace mlir::db;

namespace {

// The lookups compare as id() does, which an entity compared to itself agrees with on every
// value but a double: n = 3.0 holds of no node, id(n) = 3.0 of node 3
bool mayHoldADouble(Type element) {
    return isa<Float64Type, storage::AnyType>(element);
}

void addBoundIDs(Value bound, EntityIDList& entityIDs) {
    if (!llvm::is_contained(entityIDs._bound, bound)) {
        entityIDs._bound.push_back(bound);
    }
}

// A value bound when the query runs that an entity of @param scanColumn equals by its ID: an
// entity of the same kind, or an integer
bool matchBoundID(Value value, Value scanColumn, EntityIDList& entityIDs) {
    if (!::db::isRunInvariant(value)) {
        return false;
    }

    const Type element = ::db::declaredColumnElement(value);
    const Type scanned = entityElementOf(scanColumn);
    if (!element.isSignlessInteger(64) && element != scanned) {
        return false;
    }

    addBoundIDs(value, entityIDs);

    return true;
}

bool matchBoundIDList(Value list, bool comparesTheID, EntityIDList& entityIDs) {
    if (!::db::isRunInvariant(list)) {
        return false;
    }

    const storage::ListType listType = dyn_cast<storage::ListType>(::db::declaredColumnElement(list));
    if (!listType || (!comparesTheID && mayHoldADouble(listType.getElementType()))) {
        return false;
    }

    addBoundIDs(list, entityIDs);

    return true;
}

// An entity equals an integer literal by its ID and no other literal: a double it would equal
// through id() was canonicalized to the integer
bool matchIDEquality(Value mask, Value scanColumn, EntityIDList& entityIDs) {
    EqOp equality = mask.getDefiningOp<EqOp>();
    if (!equality) {
        return false;
    }

    const Value lhs = equality.getLhs();
    const Value rhs = equality.getRhs();

    Value value;
    if (lhs == scanColumn) {
        value = rhs;
    } else if (rhs == scanColumn) {
        value = lhs;
    } else {
        return false;
    }

    ConstantOp constant = value.getDefiningOp<ConstantOp>();
    if (!constant) {
        return matchBoundID(value, scanColumn, entityIDs);
    }

    const IntegerAttr integer = dyn_cast<IntegerAttr>(constant.getValue());
    int64_t entityID = 0;
    if (integer && entityIDOfLiteral(integer, entityID)) {
        entityIDs._literals.push_back(entityID);
    }

    return true;
}

bool matchIDMembership(Value mask, Value scanColumn, EntityIDList& entityIDs) {
    InOp membership = mask.getDefiningOp<InOp>();
    if (!membership || comparedEntityColumn(membership.getLhs()) != scanColumn) {
        return false;
    }

    const bool comparesTheID = static_cast<bool>(membership.getLhs().getDefiningOp<ElementID>());

    const Value list = membership.getRhs();
    ConstantOp constant = list.getDefiningOp<ConstantOp>();
    if (!constant) {
        return matchBoundIDList(list, comparesTheID, entityIDs);
    }

    const ArrayAttr elements = dyn_cast<ArrayAttr>(constant.getValue());
    if (!elements) {
        return false;
    }

    const bool listsADouble = llvm::any_of(elements, [](Attribute element) {
        return isa<FloatAttr>(element);
    });

    if (!comparesTheID && listsADouble) {
        return false;
    }

    for (const Attribute element : elements) {
        int64_t entityID = 0;
        if (entityIDOfLiteral(element, entityID)) {
            entityIDs._literals.push_back(entityID);
        }
    }

    return true;
}

// A mask a filter keeps no row on: one of its conjuncts is false, or null on every row
bool isNeverTrue(Value mask) {
    llvm::SmallVector<Value> conjuncts;
    collectConjuncts(mask, conjuncts);

    return llvm::any_of(conjuncts, [](Value conjunct) {
        ConstantOp constant = conjunct.getDefiningOp<ConstantOp>();
        const BoolAttr truth = constant ? dyn_cast<BoolAttr>(constant.getValue()) : BoolAttr();
        const bool isFalse = truth && !truth.getValue();

        return isFalse || isUntypedNullMask(conjunct);
    });
}

}

void mlir::db::computeBoundValuesAhead(Operation* lookup) {
    mlir::IRRewriter rewriter(lookup->getContext());
    const mlir::LogicalResult moved = mlir::moveValueDefinitions(rewriter, lookup->getOperands(), lookup);

    bioassert(mlir::succeeded(moved), "The bound values of a lookup are computed ahead of it");
}

Value mlir::db::comparedEntityColumn(Value side) {
    if (ElementID id = side.getDefiningOp<ElementID>()) {
        side = id.getInput();
    }

    return isEntityColumn(side) ? side : Value();
}

bool mlir::db::entityIDOfLiteral(Attribute literal, int64_t& entityID) {
    const IntegerAttr integer = dyn_cast<IntegerAttr>(literal);
    const FloatAttr floating = dyn_cast<FloatAttr>(literal);

    if (integer && integer.getType().isSignlessInteger(64)) {
        return ::db::entityIDOfNumber(integer.getInt(), entityID);
    } else if (floating) {
        return ::db::entityIDOfNumber(floating.getValueAsDouble(), entityID);
    }

    return false;
}

bool mlir::db::collectIDDisjunction(Value mask, Value scanColumn, EntityIDList& entityIDs) {
    llvm::SmallVector<Value> pending {mask};
    while (!pending.empty()) {
        const Value disjunct = pending.pop_back_val();

        if (OrOp disjunction = disjunct.getDefiningOp<OrOp>()) {
            pending.push_back(disjunction.getRhs());
            pending.push_back(disjunction.getLhs());
            continue;
        }

        const bool listsEntities = matchIDEquality(disjunct, scanColumn, entityIDs)
                                || matchIDMembership(disjunct, scanColumn, entityIDs)
                                || isNeverTrue(disjunct);
        if (!listsEntities) {
            return false;
        }
    }

    llvm::SmallVector<int64_t>& literals = entityIDs._literals;
    llvm::sort(literals);
    literals.erase(std::unique(literals.begin(), literals.end()), literals.end());

    return true;
}

bool mlir::db::matchPropertyEquality(EqOp equality, PropertyEquality& match) {
    const Value lhs = equality.getLhs();
    const Value rhs = equality.getRhs();

    GetNodeProperties read = lhs.getDefiningOp<GetNodeProperties>();
    Value constantSide = rhs;
    if (!read) {
        read = rhs.getDefiningOp<GetNodeProperties>();
        constantSide = lhs;
    }

    if (!read) {
        return false;
    }

    ConstantOp constant = constantSide.getDefiningOp<ConstantOp>();
    const TypedAttr literal = constant ? dyn_cast<TypedAttr>(constant.getValue()) : TypedAttr();
    const bool scansTheLiteral = literal && storage::isPropertyScanLiteral(literal);
    const bool scansTheBoundValue = !constant
                                 && ::db::isRunInvariant(constantSide)
                                 && storage::isPropertyScanType(::db::declaredColumnElement(constantSide));

    if (!scansTheLiteral && !scansTheBoundValue) {
        return false;
    }

    match._read = read;
    match._value = literal;
    match._boundValue = scansTheBoundValue ? constantSide : Value();
    return true;
}

Value mlir::db::disjunctionColumn(Value mask) {
    llvm::SmallVector<Value> pending {mask};
    while (!pending.empty()) {
        const Value disjunct = pending.pop_back_val();

        if (OrOp disjunction = disjunct.getDefiningOp<OrOp>()) {
            pending.push_back(disjunction.getRhs());
            pending.push_back(disjunction.getLhs());
        } else if (InOp membership = disjunct.getDefiningOp<InOp>()) {
            return comparedEntityColumn(membership.getLhs());
        } else if (EqOp equality = disjunct.getDefiningOp<EqOp>()) {
            const Value lhs = equality.getLhs();
            const Value rhs = equality.getRhs();

            if (isEntityColumn(lhs)) {
                return lhs;
            } else if (isEntityColumn(rhs)) {
                return rhs;
            }

            return {};
        } else if (!isNeverTrue(disjunct)) {
            return {};
        }
    }

    return {};
}

Value mlir::db::nodeDisjunctionColumn(Value mask) {
    const Value column = disjunctionColumn(mask);

    return column && isNodeColumn(column) ? column : Value();
}

bool mlir::db::matchWalkEndEdge(Value column, WalkEndEdge& endEdge) {
    Operation* const read = column.getDefiningOp();

    bool readsTheFront = false;
    if (isa_and_nonnull<Head>(read)) {
        readsTheFront = true;
    } else if (ListIndex index = dyn_cast_or_null<ListIndex>(read)) {
        ConstantOp position = index.getRhs().getDefiningOp<ConstantOp>();
        const IntegerAttr integer = position ? dyn_cast<IntegerAttr>(position.getValue()) : IntegerAttr();
        const bool readsAnEnd = integer && (integer.getInt() == 0 || integer.getInt() == -1);
        if (!readsAnEnd) {
            return false;
        }

        readsTheFront = integer.getInt() == 0;
    } else if (!isa_and_nonnull<Last>(read)) {
        return false;
    }

    ExpandPath expansion = read->getOperand(0).getDefiningOp<ExpandPath>();
    const bool listsEveryEdge = expansion && expansion.getKind() == storage::PathExpansionKind::Edges && !expansion.getStep();
    if (!listsEveryEdge) {
        return false;
    }

    ExplorePaths walk = climbFilters(expansion.getPaths()).getDefiningOp<ExplorePaths>();
    if (!walk) {
        return false;
    }

    endEdge._walk = walk;
    endEdge._first = readsTheFront != expansion.getReversed();

    return true;
}

ConstScanEdges mlir::db::scanListedEdges(const EntityIDList& edgeIDs, bool undirected, Location loc, mlir::OpBuilder& builder) {
    mlir::MLIRContext* const context = builder.getContext();
    const Type nodeType = ColumnType::get(context, storage::NodeIDType::get(context));
    const Type edgeType = ColumnType::get(context, storage::EdgeIDType::get(context));
    const Type edgeTypeType = ColumnType::get(context, storage::EdgeTypeIDType::get(context));

    ConstScanEdges listed = builder.create<ConstScanEdges>(loc,
                                                           nodeType,
                                                           edgeType,
                                                           edgeTypeType,
                                                           nodeType,
                                                           edgeIDs._literals,
                                                           edgeIDs._bound,
                                                           undirected);
    computeBoundValuesAhead(listed);

    return listed;
}

Value mlir::db::scanListedEdgeEnds(const EntityIDList& edgeIDs, storage::PathDirection direction, bool reached, Location loc, mlir::OpBuilder& builder) {
    ConstScanEdges listed = scanListedEdges(edgeIDs, direction == storage::PathDirection::Both, loc, builder);
    const bool readsTheSources = (direction == storage::PathDirection::Backward) == reached;

    return readsTheSources ? listed.getSrcids() : listed.getTgtids();
}
