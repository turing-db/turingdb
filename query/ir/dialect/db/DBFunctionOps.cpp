#include "DBOps.h"

#include "llvm/ADT/STLExtras.h"

#include "DBColumnChecks.h"
#include "DBRowCosts.h"
#include "DBSpeculation.h"
#include "BoundEntityIDs.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

namespace {

Speculation::Speculatability speculatableWhenReadingPoints(Operation* op) {
    const bool readsPoints = llvm::all_of(op->getOperands(), [](Value operand) {
        return isa<storage::PointType>(columnElement(operand));
    });

    return speculatableWhen(readsPoints);
}

// A point function reads columns of points into a column of @param Result
template <typename Result>
LogicalResult verifyPointFunction(Operation* op, Value result, StringRef resultName) {
    const bool readsPoints = llvm::all_of(op->getOperands(), [](Value operand) {
        return isColumnOf<storage::PointType>(operand.getType());
    });

    if (!readsPoints) {
        return op->emitOpError("operands must be point columns");
    }

    const ColumnType resultColumn = dyn_cast<ColumnType>(result.getType());
    if (!resultColumn || !isa<Result>(resultColumn.getType())) {
        return op->emitOpError("result must be a column of ") << resultName;
    }

    return success();
}

}

LogicalResult EntityIDOf::verify() {
    const Type element = columnElement(getInput());
    if (!storage::isEntityIDOfInputType(element)) {
        return emitOpError("requires a value, a node or an edge, not a path, got ") << element;
    }

    return success();
}

OpFoldResult EntityIDOf::fold(FoldAdaptor adaptor) {
    const Value input = getInput();
    const Type resultType = getType();

    if (input.getType() == resultType) {
        return input;
    }

    ElementID id = input.getDefiningOp<ElementID>();
    if (id && id.getInput().getType() == resultType) {
        return id.getInput();
    }

    return {};
}

LogicalResult Replace::verify() {
    const bool readsStrings = llvm::all_of(getOperation()->getOperands(), [](Value operand) {
        return isColumnOf<storage::StringType>(operand.getType());
    });

    if (!readsStrings) {
        return emitOpError("operands must be string columns");
    }

    return verifyStringResult(getOperation(), getResult());
}

LogicalResult PointDistance::verify() {
    return verifyPointFunction<Float64Type>(getOperation(), getResult(), "f64");
}

LogicalResult PointWithinBBox::verify() {
    return verifyPointFunction<storage::BoolType>(getOperation(), getResult(), "booleans");
}

LogicalResult Split::verify() {
    const Value delimiter = getDelimiter();

    const bool readsAString = isColumnOf<storage::StringType>(getString().getType());
    const bool readsDelimiters = isColumnOf<storage::StringType>(delimiter.getType()) || isa<storage::ListType>(columnElement(delimiter));
    if (!readsAString || !readsDelimiters) {
        return emitOpError("operands must be a string column, then a string or list column");
    }

    const ColumnType resultColumn = llvm::dyn_cast<ColumnType>(getResult().getType());
    const storage::ListType listType = resultColumn ? llvm::dyn_cast<storage::ListType>(resultColumn.getType())
                                                    : storage::ListType {};

    if (!listType || !llvm::isa<storage::StringType>(listType.getElementType())) {
        return emitOpError("result must be a column of string lists");
    }

    return success();
}

Speculation::Speculatability Substring::getSpeculatability() {
    return speculatableOverCharacters(getOperation());
}

Speculation::Speculatability Replace::getSpeculatability() {
    const bool readsStrings = llvm::all_of(getOperation()->getOperands(), [](Value operand) {
        return isa<storage::StringType>(columnElement(operand));
    });

    return speculatableWhen(readsStrings);
}

Speculation::Speculatability PointDistance::getSpeculatability() {
    return speculatableWhenReadingPoints(getOperation());
}

Speculation::Speculatability PointWithinBBox::getSpeculatability() {
    return speculatableWhenReadingPoints(getOperation());
}

Speculation::Speculatability Split::getSpeculatability() {
    const bool readsAString = isa<storage::StringType>(columnElement(getString()));

    const Type delimiter = columnElement(getDelimiter());
    const storage::ListType delimiterList = dyn_cast<storage::ListType>(delimiter);
    const Type delimiterElement = delimiterList ? delimiterList.getElementType() : delimiter;
    const bool splitsOnStrings = isa<storage::StringType>(delimiterElement);

    return speculatableWhen(readsAString && splitsOnStrings);
}

double Labels::getRowCost() {
    return STORAGE_READ_ROW_COST + CONTAINER_BUILD_ROW_COST;
}

double EdgeType::getRowCost() {
    return STORAGE_READ_ROW_COST;
}

double Keys::getRowCost() {
    return STORAGE_READ_ROW_COST + CONTAINER_BUILD_ROW_COST;
}

double PropertyMap::getRowCost() {
    return STORAGE_READ_ROW_COST + CONTAINER_BUILD_ROW_COST;
}

double StartNode::getRowCost() {
    return STORAGE_READ_ROW_COST;
}

double EndNode::getRowCost() {
    return STORAGE_READ_ROW_COST;
}

double CosineSimilarity::getRowCost() {
    return VECTOR_FUNCTION_ROW_COST;
}

double EuclideanDistance::getRowCost() {
    return VECTOR_FUNCTION_ROW_COST;
}
