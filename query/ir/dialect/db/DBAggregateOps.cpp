#include "DBOps.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"

#include "StorageEnums.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

LogicalResult CountScanRows::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one scan to count");
    }

    const std::optional<llvm::StringRef> property = getProperty();
    if (property && property->empty()) {
        return emitOpError("requires a non-empty property name");
    }

    if (!property && getPropertyScan()) {
        return emitOpError("names the scan of a property it does not read");
    }

    if (getPropertyScan().value_or(0) >= getLabels().size()) {
        return emitOpError("reads a property from a scan it does not list");
    }

    return success();
}

// db.group_aggregate splits its columns into keyCount grouping keys followed by
// one input per aggregate, so keyCount and kinds must partition the columns with
// at least one of each. Its results are the key columns (passed through) then one
// per aggregate, so the key results must match their key columns; the aggregate
// result types are resolved during lowering and left unconstrained (as db.count).
LogicalResult GroupAggregate::verify() {
    const OperandRange columns = getColumns();
    const Operation::result_range results = getResults();
    const uint64_t keyCount = getKeyCount();
    const ArrayRef<int64_t> kinds = getKinds();

    // A grouped aggregate with no key is a whole-stream aggregate (db.count /
    // db.sum ...); with no aggregate it is a projection or DISTINCT. Either way it
    // is not this op, so require at least one of each.
    if (keyCount == 0) {
        return emitOpError("requires at least one grouping key");
    }

    if (kinds.empty()) {
        return emitOpError("requires at least one aggregate");
    }

    // Every column is either a grouping key or one aggregate's input, so the key and
    // aggregate counts must partition the columns exactly. keyCount is an unbounded
    // attribute, so bound it by the real column count first and compare the remainder
    // to kinds.size(): summing keyCount + kinds.size() would wrap in unsigned 64-bit
    // and let a pathological keyCount slip past the check into out-of-bounds lowering.
    const size_t columnCount = columns.size();
    if (keyCount > columnCount || columnCount - keyCount != kinds.size()) {
        return emitOpError("expects ") << keyCount
                                       << " grouping-key columns and " << kinds.size()
                                       << " aggregate columns, but has " << columnCount;
    }

    // One result per grouping key then one per aggregate.
    if (results.size() != columns.size()) {
        return emitOpError("expects ") << columns.size()
                                       << " results, one per grouping key and aggregate, but has "
                                       << results.size();
    }

    // The grouping keys pass through unchanged, so each key result keeps its key
    // column's type. The aggregate result types are resolved during lowering.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        if (columns[keyIndex].getType() != results[keyIndex].getType()) {
            return emitOpError("grouping-key result ") << keyIndex
                                                       << " must have the same type as key column "
                                                       << keyIndex;
        }
    }

    // Each kind names one aggregate's reduction, so it must be a valid
    // GroupAggregateKind (count / count_distinct / sum / sum_distinct / min / max /
    // avg / avg_distinct).
    for (const int64_t kind : kinds) {
        if (!storage::symbolizeGroupAggregateKind(kind)) {
            return emitOpError("has an unknown aggregate kind ") << kind;
        }
    }

    return success();
}

// db.collect splits its columns into keyCount grouping keys followed by exactly one
// collected value column, so operands are keyCount + 1. Its results are the key
// columns (passed through) then one list column holding the collected values.
LogicalResult Collect::verify() {
    const OperandRange columns = getColumns();
    const Operation::result_range results = getResults();
    const uint64_t keyCount = getKeyCount();

    // The collected columns follow the grouping keys, and one aggregate input follows
    // them per kind, so what is left over after the keys and the aggregates is the value
    // columns - at least one. Bound keyCount by the real column count first: summing
    // keyCount + aggregateCount could wrap in unsigned 64-bit and let a pathological
    // keyCount slip past into out-of-bounds lowering.
    const size_t columnCount = columns.size();
    const llvm::ArrayRef<int64_t> kinds = getKinds().value_or(llvm::ArrayRef<int64_t> {});
    const size_t aggregateCount = kinds.size();
    if (keyCount >= columnCount || columnCount - keyCount <= aggregateCount) {
        return emitOpError("expects ") << keyCount
                                       << " grouping-key columns, at least one collected column and "
                                       << aggregateCount
                                       << " aggregate columns, but has "
                                       << columnCount;
    }

    const size_t valueCount = columnCount - keyCount - aggregateCount;

    // One result per grouping key, one per collected list, then one per aggregate.
    if (results.size() != columnCount) {
        return emitOpError("expects ") << columnCount
                                       << " results, one per grouping key plus the collected lists and the "
                                          "aggregates, but has "
                                       << results.size();
    }

    // The grouping keys pass through unchanged, so each key result keeps its key
    // column's type.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        if (columns[keyIndex].getType() != results[keyIndex].getType()) {
            return emitOpError("grouping-key result ") << keyIndex
                                                       << " must have the same type as key column "
                                                       << keyIndex;
        }
    }

    // Each value's result is the list it collects into: a column whose element type is a
    // storage list of that value column's type.
    for (size_t valueIndex = 0; valueIndex < valueCount; valueIndex++) {
        const size_t position = keyCount + valueIndex;

        const ColumnType listColumn = llvm::dyn_cast<ColumnType>(results[position].getType());
        if (!listColumn || !llvm::isa<storage::ListType>(listColumn.getType())) {
            return emitOpError("collected result ") << valueIndex << " must be a list column";
        }

        const ColumnType valueColumn = llvm::cast<ColumnType>(columns[position].getType());
        const storage::ListType collectedList = llvm::cast<storage::ListType>(listColumn.getType());
        if (collectedList.getElementType() != valueColumn.getType()) {
            return emitOpError("collected result ") << valueIndex
                                                    << " must be a list of "
                                                    << valueColumn.getType()
                                                    << ", the collected column's type, but collects "
                                                    << collectedList.getElementType();
        }
    }

    // Each kind names one aggregate's reduction, so it must be a valid
    // GroupAggregateKind, exactly as db.group_aggregate's kinds must.
    for (const int64_t kind : kinds) {
        if (!storage::symbolizeGroupAggregateKind(kind)) {
            return emitOpError("has an unknown aggregate kind ") << kind;
        }
    }

    // Every deduplicating value names one of the collected columns.
    if (const std::optional<llvm::ArrayRef<int64_t>> distinctValues = getDistinctValues()) {
        for (const int64_t valueIndex : *distinctValues) {
            if (valueIndex < 0 || static_cast<size_t>(valueIndex) >= valueCount) {
                return emitOpError("distinct value index ") << valueIndex
                                                            << " is out of range for "
                                                            << valueCount
                                                            << " collected columns";
            }
        }
    }

    return success();
}

// db.unwind_collect has the same column shape as db.collect - keyCount grouping keys
// then one collected value column - but re-emits one scalar row per element, so its
// results are the key columns (passed through) then one value column.
LogicalResult UnwindCollect::verify() {
    const OperandRange columns = getColumns();
    const Operation::result_range results = getResults();
    const uint64_t keyCount = getKeyCount();

    const size_t columnCount = columns.size();
    if (keyCount >= columnCount || columnCount - keyCount != 1) {
        return emitOpError("expects ") << keyCount
                                       << " grouping-key columns and one collected column, but has "
                                       << columnCount;
    }

    if (results.size() != columnCount) {
        return emitOpError("expects ") << columnCount
                                       << " results, one per grouping key plus the unwound value, but has "
                                       << results.size();
    }

    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        if (columns[keyIndex].getType() != results[keyIndex].getType()) {
            return emitOpError("grouping-key result ") << keyIndex
                                                       << " must have the same type as key column "
                                                       << keyIndex;
        }
    }

    if (columns[keyCount].getType() != results[keyCount].getType()) {
        return emitOpError("unwound value result must have the same type as the collected column");
    }

    return success();
}

bool GroupAggregate::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._count = getKeyCount()};
    return true;
}

bool Collect::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._count = getKeyCount()};
    return true;
}

OperandRange GroupAggregate::getGroupingKeys() {
    return getColumns().take_front(getKeyCount());
}

OperandRange Collect::getGroupingKeys() {
    return getColumns().take_front(getKeyCount());
}
