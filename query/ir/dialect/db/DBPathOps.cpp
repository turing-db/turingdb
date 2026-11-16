#include "DBOps.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "DBColumnChecks.h"
#include "DBRegionColumns.h"
#include "IRConstantColumn.h"
#include "StorageEnums.h"
#include "PathHopArguments.h"
#include "MakePathShape.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

namespace {

constexpr size_t PATH_FIXED_RESULT_COUNT = 3;

// What a filter reads as a mask: a boolean column, a boolean literal, one whose values may be
// null, a property read whose type lowering resolves, or the untyped null an expression over a
// property no row carries evaluates to
bool isMaskColumn(Type type) {
    const auto column = dyn_cast<ColumnType>(type);
    if (!column) {
        return false;
    }

    const Type element = column.getType();
    if (isa<storage::BoolType>(element) || isa<NoneType>(element) || element.isInteger(1)) {
        return true;
    }

    const auto nullable = dyn_cast<storage::NullableType>(element);
    if (!nullable) {
        return false;
    }

    const Type value = nullable.getValueType();

    return isa<storage::BoolType>(value) || isa<NoneType>(value) || value.isInteger(1);
}

// One list of names per step of a path exploration, an empty list leaving its step free
LogicalResult verifyStepNames(Operation* operation, ArrayAttr names, size_t stepCount, llvm::StringRef what) {
    if (!names) {
        return success();
    } else if (names.size() != stepCount) {
        return operation->emitOpError(what) << " must hold one list per step";
    }

    bool namesAny = false;
    for (const Attribute stepNames : names) {
        for (const Attribute name : cast<ArrayAttr>(stepNames)) {
            const StringAttr text = dyn_cast<StringAttr>(name);
            if (text && text.getValue().empty()) {
                return operation->emitOpError(what) << " must not hold an empty name";
            }

            namesAny = true;
        }
    }

    if (!namesAny) {
        return operation->emitOpError(what) << " must name at least one name";
    }

    return success();
}

// The names of one step of a path exploration, null when the step leaves them free
ArrayAttr stepNamesAt(ArrayAttr names, size_t step) {
    if (!names) {
        return {};
    }

    const ArrayAttr stepNames = cast<ArrayAttr>(names[step]);

    return stepNames.empty() ? ArrayAttr() : stepNames;
}

// The hop region of one step of an explore_paths: one block over the nodes and edges of the
// repetition up to the step's end node yielding a boolean column, reading nothing defined
// outside it but constants
LogicalResult verifyHopRegion(Operation* op, Region& hop, size_t step, ValueRange imports) {
    Block& block = hop.front();
    MLIRContext* context = op->getContext();

    const Type nodeColumn = ColumnType::get(context, storage::NodeIDType::get(context));
    const Type edgeColumn = ColumnType::get(context, storage::EdgeIDType::get(context));
    if (failed(verifyHopArguments(op, block, step, nodeColumn, edgeColumn, imports))) {
        return failure();
    }

    Yield yield = getFactorYield(hop);
    if (!yield) {
        return op->emitOpError("hop region must end with a db.yield");
    }

    const bool yieldsOneMask = yield.getColumns().size() == 1 && isMaskColumn(yield.getColumns().front().getType());
    if (!yieldsOneMask) {
        return op->emitOpError("hop region must yield exactly one boolean column");
    }

    for (Operation& inner : block) {
        for (const Value operand : inner.getOperands()) {
            if (const auto argument = dyn_cast<BlockArgument>(operand)) {
                if (argument.getOwner() != &block && !::db::isFunctionArgument(argument)) {
                    return op->emitOpError("hop region reads a block argument of another region");
                }

                continue;
            }

            Operation* const definingOp = operand.getDefiningOp();
            const bool definedInside = definingOp->getBlock() == &block;
            if (!definedInside && !::db::yieldsConstantColumn(operand)) {
                return op->emitOpError("hop region may only read constants from outside itself");
            }
        }
    }

    return success();
}

}

void ExplorePaths::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

storage::PathDirection ExplorePaths::getStepDirection(size_t step) {
    return static_cast<storage::PathDirection>(getDirections()[step]);
}

ArrayAttr ExplorePaths::getStepEdgeTypes(size_t step) {
    return stepNamesAt(getEdgeTypesAttr(), step);
}

ArrayAttr ExplorePaths::getStepHopLabels(size_t step) {
    return stepNamesAt(getHopLabelsAttr(), step);
}

LogicalResult ExplorePaths::verify() {
    const OperandRange carried = getColumnsToFilter();
    const ResultRange filtered = getFilteredColumns();
    if (carried.size() != filtered.size()) {
        return emitOpError("expects one filtered column per carried column, but carries ")
               << carried.size() << " and filters " << filtered.size();
    }

    for (size_t columnIndex = 0; columnIndex < carried.size(); columnIndex++) {
        if (carried[columnIndex].getType() != filtered[columnIndex].getType()) {
            return emitOpError("filtered column ") << columnIndex
                                                   << " must have the type of carried column "
                                                   << columnIndex;
        }
    }

    const std::optional<uint64_t> maxHops = getMaxHops();
    if (maxHops && *maxHops < getMinHops()) {
        return emitOpError("max_hops must be at least min_hops");
    }

    MutableArrayRef<Region> hops = getHops();
    if (failed(verifyPathSteps(getOperation(), getDirections(), hops.size()))) {
        return failure();
    }

    const size_t stepCount = getStepCount();
    const bool stepNamesFail = failed(verifyStepNames(getOperation(), getEdgeTypesAttr(), stepCount, "edge_types"))
                            || failed(verifyStepNames(getOperation(), getHopLabelsAttr(), stepCount, "hop_labels"));
    if (stepNamesFail) {
        return failure();
    }

    if (const std::optional<ArrayAttr> endLabels = getEndLabels()) {
        if (endLabels->empty()) {
            return emitOpError("end_labels must name at least one label");
        }

        for (const Attribute label : *endLabels) {
            const StringAttr name = dyn_cast<StringAttr>(label);
            if (name && name.getValue().empty()) {
                return emitOpError("end_labels must name labels");
            }
        }
    }

    if (const std::optional<uint64_t> endColumn = getEndColumn()) {
        if (*endColumn >= carried.size()) {
            return emitOpError("end_column ") << *endColumn << " is not a carried column";
        }

        const auto endType = cast<ColumnType>(carried[*endColumn].getType());
        if (!isa<storage::NodeIDType>(endType.getType())) {
            return emitOpError("end_column must name a node column");
        }
    }

    if (getEndsOnSeed() && getEndColumn()) {
        return emitOpError("ends_on_seed names the end already named by end_column");
    }

    if (getEndNodes() && (getEndColumn() || getEndsOnSeed())) {
        return emitOpError("end_nodes names the end already named by end_column or ends_on_seed");
    }

    if (getDistinct()) {
        if (!getPaths().use_empty()) {
            return emitOpError("distinct emits no path, so paths must have no use");
        }
    }

    const OperandRange imports = getHopImports();

    const bool hasPredicate = hasHopPredicate(hops);
    if (!hasPredicate) {
        if (!imports.empty()) {
            return emitOpError("hop imports without a hop region to read them");
        }

        return success();
    }

    if (getDistinct() && !imports.empty()) {
        return emitOpError("a distinct search expands many seeds at one level, so it cannot "
                           "read a column that holds one value per seed");
    }

    for (size_t step = 0; step < stepCount; step++) {
        Region& hop = hops[step];
        if (!hop.empty() && failed(verifyHopRegion(getOperation(), hop, step, imports))) {
            return failure();
        }
    }

    return success();
}

LogicalResult ExpandPath::verify() {
    const auto column = dyn_cast<ColumnType>(getResult().getType());
    const auto list = column ? dyn_cast<storage::ListType>(column.getType()) : storage::ListType();
    if (!list) {
        return emitOpError("must produce a list column");
    }

    const Type elementType = list.getElementType();
    const storage::PathExpansionKind kind = getKind();

    if (kind == storage::PathExpansionKind::Edges) {
        if (!isa<storage::EdgeIDType>(elementType)) {
            return emitOpError("kind edges expands to a list of edge IDs");
        }
    } else if (!isa<storage::NodeIDType>(elementType)) {
        return emitOpError("kind sources, ends and nodes expand to a list of node IDs");
    }

    const std::optional<uint64_t> step = getStep();
    const std::optional<uint64_t> steps = getSteps();
    if (step.has_value() != steps.has_value()) {
        return emitOpError("step and steps name a step of the body together");
    } else if (steps && *step >= *steps) {
        return emitOpError("step ") << *step << " is not one of the " << *steps << " steps of the body";
    } else if (steps && kind == storage::PathExpansionKind::Nodes) {
        return emitOpError("kind nodes lists every node of the walk and takes no step");
    }

    // Reversed, the kinds swap: the pattern's end list is the walk's source list read
    // backwards, so that is the one holding the seed. The nodes hold it either way.
    const bool readsTheSeed = (kind == storage::PathExpansionKind::Sources && step.value_or(0) == 0)
                           || kind == storage::PathExpansionKind::Nodes;
    if (readsTheSeed && !getSrcids()) {
        return emitOpError("kind sources and nodes read the seed of each path from srcids");
    }

    return success();
}

LogicalResult PathLength::verify() {
    const std::optional<uint64_t> steps = getSteps();
    if (steps && *steps == 0) {
        return emitOpError("steps counts the steps of a body, so it is at least 1");
    } else if (steps && getNodes()) {
        return emitOpError("nodes counts every node of the walk, which reads no step of a body");
    }

    return success();
}

LogicalResult PathElements::verify() {
    const Type path = columnElement(getPath());
    if (!isa<storage::PathType, storage::AnyType, NoneType>(path)) {
        return emitOpError("reads a path column, a column of tagged cells, or a column whose type lowering resolves");
    }

    const auto column = dyn_cast<ColumnType>(getResult().getType());
    const auto nullable = column ? dyn_cast<storage::NullableType>(column.getType()) : storage::NullableType();
    if (!nullable) {
        return emitOpError("must produce a nullable column: the path an OPTIONAL MATCH missed reads as null");
    }

    const Type value = nullable.getValueType();
    const auto list = dyn_cast<storage::ListType>(value);
    const Type element = list ? list.getElementType() : Type();

    switch (getKind()) {
        case storage::PathElementsKind::Nodes:
            if (!isa_and_nonnull<storage::NodeIDType>(element)) {
                return emitOpError("kind nodes reads a list of node IDs");
            }
        break;

        case storage::PathElementsKind::Relationships:
            if (!isa_and_nonnull<storage::EdgeIDType>(element)) {
                return emitOpError("kind relationships reads a list of edge IDs");
            }
        break;

        case storage::PathElementsKind::Length:
            if (!value.isUnsignedInteger(64)) {
                return emitOpError("kind length reads a ui64 count");
            }
        break;
    }

    return success();
}

LogicalResult MakePath::verify() {
    llvm::SmallVector<Type> elementTypes;
    for (const Value entity : getEntities()) {
        const auto column = dyn_cast<ColumnType>(entity.getType());
        elementTypes.push_back(column ? column.getType() : Type());
    }

    return verifyMakePath(getOperation(), elementTypes, getReversedPaths());
}

bool ExplorePaths::getCarrySet(CarrySet& carrySet) {
    const MutableOperandRange carried = getCarrySetMutable();
    carrySet = CarrySet {._operandOffset = operandRunOffset(carried), ._resultOffset = PATH_FIXED_RESULT_COUNT, ._count = carried.size()};
    return true;
}

MutableOperandRange ExplorePaths::getCarrySetMutable() {
    return getColumnsToFilterMutable();
}

bool ExplorePaths::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    const MutableOperandRange carried = getRegionCarrySetMutable();
    carrySet = RegionCarrySet {._operandOffset = operandRunOffset(carried),
                               ._argumentOffset = hopWalkArgumentCount(region.getRegionNumber()),
                               ._count = carried.size(),
                               ._repeatsRows = true};
    return true;
}

MutableOperandRange ExplorePaths::getRegionCarrySetMutable() {
    return getHopImportsMutable();
}

// The seed comes back as srcids, and as tgtids too when the walk ends on it
Value ExplorePaths::getRowSource(OpResult result) {
    const bool holdsTheSeed = result == getSrcids() || (result == getTgtids() && getEndsOnSeed());
    if (holdsTheSeed) {
        return getInputNodes();
    }

    return carrySetRowSource(getOperation(), result);
}
