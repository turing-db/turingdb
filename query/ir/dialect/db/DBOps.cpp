#include "DBOps.h"

#include "DBColumnElement.h"
#include "DBColumnChecks.h"
#include "DBSpeculation.h"
#include "IRValueTypes.h"
#include "StorageEnums.h"
#include "ColumnIndicesFormat.h"
#include "EdgeDirectionsFormat.h"
#include "PathStepsFormat.h"
#include "LabelAlternativesFormat.h"
#include "StorageAttributes.h"
#include "GroupAggregateKindsFormat.h"
#include "PropertyScanLiteral.h"
#include "BoundValueFormat.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

namespace {

template <typename... ElementTypes>
Speculation::Speculatability speculatableOverElement(Value input) {
    return speculatableWhen(isa<ElementTypes...>(columnElement(input)));
}

LogicalResult verifyStringEnd(Operation* op, Value string, Value length, Value result) {
    const bool readsAStringAndALength = isColumnOf<storage::StringType>(string.getType()) && isIntegerColumn(length);
    if (!readsAStringAndALength) {
        return op->emitOpError("operands must be a string column and an integer column");
    }

    return verifyStringResult(op, result);
}

}

#define GET_OP_CLASSES
#include "DBOps.cpp.inc"

LogicalResult ImportGraph::verify() {
    if (!::db::namesImportedPropertyTypes(getFormat(), getTypesAttr())) {
        return emitOpError("types each property as a datetime, duration or point a jsonl import reads, or a duration a parquet import reads");
    }

    return success();
}

LogicalResult CreateVectorIndex::verify() {
    if (getDimension() == 0) {
        return emitOpError("must hold vectors of at least one dimension");
    }

    return success();
}
