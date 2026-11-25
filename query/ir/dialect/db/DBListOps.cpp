#include "DBOps.h"

#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/STLExtras.h"

#include "DBHeldTags.h"
#include "DBColumnChecks.h"
#include "DBRegionColumns.h"
#include "DBRowCosts.h"
#include "DBSpeculation.h"
#include "IRRange.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

namespace {

// The body of an op over the elements of one row's list: one block taking the element,
// the row tag and one argument per carried column, ending in a yield that names the tag
LogicalResult verifyElementBody(Operation* op, Block& bodyBlock, OperandRange carried) {
    auto yield = dyn_cast_or_null<ComprehensionYield>(bodyBlock.empty() ? nullptr : &bodyBlock.back());
    if (!yield) {
        return op->emitOpError("body region must end with a db.comprehension_yield");
    }

    const size_t expectedArguments = carried.size() + 2;

    if (bodyBlock.getNumArguments() != expectedArguments) {
        return op->emitOpError("body region takes the element and the row tag plus one argument per "
                               "carried column, ")
               << "expected " << expectedArguments << " but has " << bodyBlock.getNumArguments();
    }

    if (!llvm::isa<ColumnType>(bodyBlock.getArgument(0).getType())) {
        return op->emitOpError("body argument 0 must be the column of elements");
    }

    const auto rowTagType = llvm::dyn_cast<ColumnType>(bodyBlock.getArgument(1).getType());

    if (!rowTagType || !rowTagType.getType().isUnsignedInteger(64)) {
        return op->emitOpError("body argument 1 must be the ui64 column of row tags");
    }

    if (!yield.getRowTags()) {
        return op->emitOpError("body must yield the row tag of every surviving element");
    }

    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        const mlir::Type argumentType = bodyBlock.getArgument(carriedIndex + 2).getType();

        if (argumentType != carried[carriedIndex].getType()) {
            return op->emitOpError("body argument ") << carriedIndex + 2
                                                     << " must have the type of carried column "
                                                     << carriedIndex;
        }
    }

    return success();
}

// A region of a fold over the elements of one row's list: one block taking the element,
// the accumulator and one argument per carried column, ending in a @param YieldOp
template <typename YieldOp>
LogicalResult verifyFoldRegion(Operation* op, Block& block, OperandRange carried, llvm::StringLiteral yieldName) {
    if (block.empty() || !isa<YieldOp>(block.back())) {
        return op->emitOpError("region must end with a ") << yieldName;
    }

    const size_t expectedArguments = carried.size() + 2;

    if (block.getNumArguments() != expectedArguments) {
        return op->emitOpError("region takes the element and the accumulator plus one argument per "
                               "carried column, ")
               << "expected " << expectedArguments << " but has " << block.getNumArguments();
    }

    for (size_t argumentIndex = 0; argumentIndex < 2; argumentIndex++) {
        if (!llvm::isa<ColumnType>(block.getArgument(argumentIndex).getType())) {
            return op->emitOpError("argument ") << argumentIndex << " must be a column";
        }
    }

    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        const mlir::Type argumentType = block.getArgument(carriedIndex + 2).getType();

        if (argumentType != carried[carriedIndex].getType()) {
            return op->emitOpError("argument ") << carriedIndex + 2
                                                << " must have the type of carried column "
                                                << carriedIndex;
        }
    }

    return success();
}

}

// The body binds the element, the row tag and one argument per carried column, and ends
// naming what each element contributes.
LogicalResult ListComprehension::verify() {
    return verifyElementBody(getOperation(), getBody().front(), getColumnsToFilter());
}

LogicalResult ListPredicate::verify() {
    return verifyElementBody(getOperation(), getBody().front(), getColumnsToFilter());
}

LogicalResult Reduce::verify() {
    return verifyFoldRegion<ReduceYield>(getOperation(), getBody().front(), getColumnsToFilter(), "db.reduce_yield");
}

LogicalResult AllReduce::verify() {
    const OperandRange carried = getColumnsToFilter();

    if (failed(verifyFoldRegion<ReduceYield>(getOperation(), getBody().front(), carried, "db.reduce_yield"))) {
        return failure();
    }

    return verifyFoldRegion<AllReduceYield>(getOperation(), getPredicate().front(), carried, "db.all_reduce_yield");
}

// There is a column for the list to be built out of, and what the op produces is a list.
LogicalResult MakeList::verify() {
    if (getElements().empty()) {
        return emitOpError("requires at least one element column");
    }

    const ColumnType resultColumn = llvm::dyn_cast<ColumnType>(getResult().getType());
    if (!resultColumn) {
        return emitOpError("result must be a column");
    }

    if (!llvm::isa<storage::ListType>(resultColumn.getType())) {
        return emitOpError("result must be a column of lists");
    }

    return success();
}

LogicalResult MakeMap::verify() {
    const size_t valueCount = getValues().size();
    if (valueCount == 0) {
        return emitOpError("requires at least one value column");
    }

    const ArrayAttr keys = getKeys();
    if (keys.size() != valueCount) {
        return emitOpError("requires one key per value column");
    }

    llvm::SmallDenseSet<StringRef, 8> seenKeys;
    for (const Attribute key : keys) {
        if (!seenKeys.insert(llvm::cast<StringAttr>(key).getValue()).second) {
            return emitOpError("requires distinct keys");
        }
    }

    return success();
}

// A tagged cell may hold a map, and a column whose type lowering resolves may be one
LogicalResult MergeMaps::verify() {
    const OperandRange maps = getMaps();
    if (maps.size() < 2) {
        return emitOpError("requires at least two map columns");
    }

    const auto readsMaps = [](Value column) {
        return isa<storage::MapType, storage::AnyType, NoneType>(columnElement(column));
    };

    if (!llvm::all_of(maps, readsMaps)) {
        return emitOpError("operands must be columns of maps");
    }

    return success();
}

// The integers a range spans ride the list it builds, so the result is a column of them.
LogicalResult Range::verify() {
    const ColumnType resultColumn = llvm::dyn_cast<ColumnType>(getResult().getType());
    if (!resultColumn) {
        return emitOpError("result must be a column");
    }

    const storage::ListType listType = llvm::dyn_cast<storage::ListType>(resultColumn.getType());
    if (!listType) {
        return emitOpError("result must be a column of lists");
    }

    if (!llvm::isa<mlir::IntegerType>(listType.getElementType())) {
        return emitOpError("result must be a column of integer lists");
    }

    return success();
}

LogicalResult SortMulti::verify() {
    const Value fields = getFields();
    const Value limit = getLimit();
    const Value skip = getSkip();

    const bool readsLists = isColumnOf<storage::ListType>(getList().getType())
                         && (!fields || isColumnOf<storage::ListType>(fields.getType()));
    const bool readsIntegerBounds = (!limit || isIntegerColumn(limit)) && (!skip || isIntegerColumn(skip));

    if (!readsLists) {
        return emitOpError("sorts a column of lists on a column of lists of fields");
    } else if (!readsIntegerBounds) {
        return emitOpError("limit and skip must be integer columns");
    }

    return success();
}

bool ListComprehension::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    fillTrailingRegionCarrySet(getOperation(), region, getColumnsToFilter().size(), carrySet);
    return true;
}

bool ListPredicate::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    fillTrailingRegionCarrySet(getOperation(), region, getColumnsToFilter().size(), carrySet);
    return true;
}

bool Reduce::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    fillTrailingRegionCarrySet(getOperation(), region, getColumnsToFilter().size(), carrySet);
    return true;
}

bool AllReduce::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    fillTrailingRegionCarrySet(getOperation(), region, getColumnsToFilter().size(), carrySet);
    return true;
}

Speculation::Speculatability Range::getSpeculatability() {
    int64_t from = 0;
    int64_t to = 0;
    int64_t by = 1;

    const Value step = getStep();
    const bool literalBounds = matchIntegerLiteral(getStart(), from) && matchIntegerLiteral(getEnd(), to);
    const bool literalStep = !step || matchIntegerLiteral(step, by);
    const bool knownList = literalBounds && literalStep && by != 0;

    if (!knownList) {
        return Speculation::NotSpeculatable;
    }

    return speculatableWhen(::db::countRangeSteps(from, to, by) < ::db::RANGE_LENGTH_LIMIT);
}

Speculation::Speculatability ListSlice::getSpeculatability() {
    const Value from = getFrom();
    const Value to = getTo();
    const bool readsIntegerBounds = (!from || holdsOnlyIntegers(from)) && (!to || holdsOnlyIntegers(to));

    return speculatableWhen(holdsOnlyLists(getList()) && readsIntegerBounds);
}

// A key is sorted on in the order <=> gives cells, which raises on an embedding, and a list
// or a map may hold one at any depth. With no field the list is kept as it is.
Speculation::Speculatability SortMulti::getSpeculatability() {
    const Value list = getList();
    const Value fields = getFields();
    const Value limit = getLimit();
    const Value skip = getSkip();

    int64_t skipped = 0;
    const bool skipsALiteralCount = !skip || (matchIntegerLiteral(skip, skipped) && skipped >= 0);
    const bool readsIntegerLimit = !limit || holdsOnlyIntegers(limit);

    ConstantOp fieldsLiteral = fields ? fields.getDefiningOp<ConstantOp>() : ConstantOp {};
    const ArrayAttr names = fieldsLiteral ? dyn_cast<ArrayAttr>(fieldsLiteral.getValue()) : ArrayAttr {};

    if (!skipsALiteralCount || !readsIntegerLimit || !holdsOnlyLists(list)) {
        return Speculation::NotSpeculatable;
    } else if (!fields || (names && names.empty())) {
        return Speculation::Speculatable;
    } else if (!names || !::db::readsHoldOnlyNullOr(list, {StringAttr {}}, {::db::CellTag::MapView})) {
        return Speculation::NotSpeculatable;
    }

    for (const Attribute name : names) {
        const StringAttr field = dyn_cast<StringAttr>(name);
        if (!field) {
            return Speculation::NotSpeculatable;
        }

        const StringRef text = field.getValue();
        const StringAttr key = StringAttr::get(getContext(), text.starts_with("^") ? text.drop_front() : text);
        const StringAttr keyOfEachElement[] = {StringAttr {}, key};

        const bool keyHasAnOrder = ::db::readsHoldOnlyNullOr(list,
                                                             keyOfEachElement,
                                                             {::db::CellTag::Int,
                                                              ::db::CellTag::UInt,
                                                              ::db::CellTag::Double,
                                                              ::db::CellTag::String,
                                                              ::db::CellTag::Bool,
                                                              ::db::CellTag::DateTime,
                                                              ::db::CellTag::Duration,
                                                              ::db::CellTag::Point,
                                                              ::db::CellTag::NodeID,
                                                              ::db::CellTag::EdgeID,
                                                              ::db::CellTag::Path});

        if (!keyHasAnOrder) {
            return Speculation::NotSpeculatable;
        }
    }

    return Speculation::Speculatable;
}

Speculation::Speculatability ListIndex::getSpeculatability() {
    return speculatableWhen(holdsOnlyLists(getLhs()));
}

Speculation::Speculatability StaticMapKey::getSpeculatability() {
    return speculatableWhen(holdsOnlyKeyedValues(getMap()));
}

Speculation::Speculatability DynamicMapKey::getSpeculatability() {
    return speculatableWhen(holdsOnlyKeyedValues(getMap()));
}

double MakeList::getRowCost() {
    return CONTAINER_BUILD_ROW_COST;
}

double MakeMap::getRowCost() {
    return CONTAINER_BUILD_ROW_COST;
}

double MergeMaps::getRowCost() {
    return CONTAINER_BUILD_ROW_COST;
}

double Range::getRowCost() {
    return CONTAINER_BUILD_ROW_COST;
}

double ListSlice::getRowCost() {
    return CONTAINER_BUILD_ROW_COST;
}
