#include "DBOps.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "DBColumnElement.h"
#include "DBColumnChecks.h"
#include "DBRegionColumns.h"
#include "DBSpeculation.h"
#include "IRValueTypes.h"
#include "StorageEnums.h"
#include "ColumnIndicesFormat.h"
#include "EdgeDirectionsFormat.h"
#include "PathStepsFormat.h"
#include "LabelAlternativesFormat.h"
#include "StorageAttributes.h"
#include "MergePatternShape.h"
#include "NullLiteralType.h"
#include "GroupAggregateKindsFormat.h"
#include "PropertyScanLiteral.h"
#include "BoundValueFormat.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

namespace {

bool writesAMapPerRow(Value column) {
    const Type declared = cast<ColumnType>(column.getType()).getType();

    return mayHoldAnyType(column) || isa<storage::MapType>(columnElement(column)) || isNullLiteralType(declared);
}

template <typename IDType>
bool holdsIDsOf(Value column) {
    return mayHoldAnyType(column) || isa<IDType>(columnElement(column));
}

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

// A body run once per row holds one value per argument for the whole run, and reads it as
// the constant it is: read directly, an argument would be a column of its own rows
LogicalResult verifyArgumentsReadAsRunConstants(Operation* subquery, Block& body) {
    for (BlockArgument argument : body.getArguments()) {
        for (Operation* user : argument.getUsers()) {
            if (!isa<RowConstant>(user)) {
                return subquery->emitOpError("a body run once per row reads argument ")
                       << argument.getArgNumber() << " through db.row_constant, not through "
                       << user->getName();
            }
        }
    }

    return success();
}

template <typename YieldOp, typename SubqueryOp>
LogicalResult verifySubqueryExpression(SubqueryOp subquery) {
    Block& bodyBlock = subquery.getBody().front();

    auto yield = dyn_cast_or_null<YieldOp>(bodyBlock.empty() ? nullptr : &bodyBlock.back());
    if (!yield) {
        return subquery.emitOpError("body region must end with a ") << YieldOp::getOperationName();
    }

    const OperandRange inputs = subquery.getInputColumns();
    const size_t inputCount = inputs.size();

    // The body carries the row tag through its dataflow only when it carries the scope,
    // and only over rows it has: a body run one row at a time, or over the single empty
    // row, is answered for by the lowering instead.
    const bool tagsRows = subquery.getCarriesScope() && inputCount > 0;

    const size_t expectedArguments = tagsRows ? inputCount + 1 : inputCount;
    const size_t argumentCount = bodyBlock.getNumArguments();

    if (argumentCount != expectedArguments) {
        return subquery.emitOpError("body region takes one argument per input column")
               << (tagsRows ? " plus the row tag" : "") << ", expected " << expectedArguments
               << " but has " << argumentCount;
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        if (bodyBlock.getArgument(inputIndex).getType() != inputs[inputIndex].getType()) {
            return subquery.emitOpError("body argument ") << inputIndex << " must have the type of input column "
                                                          << inputIndex;
        }
    }

    const bool yieldsATag = yield.getTag() != nullptr;
    if (tagsRows != yieldsATag) {
        return subquery.emitOpError("the body yields a row tag exactly when it takes one");
    }

    // A body with no tag is answered for by the rows its columns hold, so it has to hold
    // one; a tagged body is answered for by the tag, and holds whatever its clauses left.
    if (!tagsRows && yield.getColumns().empty()) {
        return subquery.emitOpError("an untagged body must yield at least one column, to answer from its rows");
    }

    if (!subquery.getCarriesScope()) {
        return verifyArgumentsReadAsRunConstants(subquery.getOperation(), bodyBlock);
    }

    return success();
}

}

#define GET_OP_CLASSES
#include "DBOps.cpp.inc"

LogicalResult CreateNode::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    if (getPropNames().size() != getPropValues().size()) {
        return emitOpError("prop_names and prop_values must have the same count, but has ")
               << getPropNames().size() << " names and " << getPropValues().size() << " values";
    }

    return success();
}

LogicalResult CreateEdge::verify() {
    const StringAttr edgeType = dyn_cast<StringAttr>(getEdgeType());
    if (edgeType && edgeType.getValue().empty()) {
        return emitOpError("requires a non-empty edge type");
    }

    if (getPropNames().size() != getPropValues().size()) {
        return emitOpError("prop_names and prop_values must have the same count, but has ")
               << getPropNames().size() << " names and " << getPropValues().size() << " values";
    }

    return success();
}

LogicalResult Merge::verify() {
    const LogicalResult pattern = verifyMergePattern(getOperation(),
                                                     getNodeLabels(),
                                                     getNodePropNames(),
                                                     getEdgeTypes(),
                                                     getEdgePropNames(),
                                                     getEdgeDirectionsAttr(),
                                                     getPendingNodesAttr(),
                                                     getBoundNodes().size(),
                                                     getBoundPending().size(),
                                                     getNodePropValues().size(),
                                                     getEdgePropValues().size());
    if (failed(pattern)) {
        return pattern;
    }

    const size_t expectedResults = mergeResultCount(getNodeLabels(), getCarriedColumns().size());
    if (getResults().size() != expectedResults) {
        return emitOpError("must produce one column per chain entity, the created mask and one "
                           "per carried column - ")
               << expectedResults << " in all, but produces " << getResults().size();
    }

    return success();
}

LogicalResult SetNodeProperty::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    return success();
}

LogicalResult SetEdgeProperty::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    return success();
}

LogicalResult SetNodeProperties::verify() {
    if (!holdsIDsOf<storage::NodeIDType>(getInputNodes())) {
        return emitOpError("writes the properties of a column of node IDs");
    } else if (!writesAMapPerRow(getValue())) {
        return emitOpError("writes a column of maps or of tagged cells, optionally nullable, or the null literal");
    } else if (!::db::namesPropertyElementTypes(getCreatedTypesAttr())) {
        return emitOpError("creates each property with the element type of a stored value");
    }

    return success();
}

LogicalResult SetEdgeProperties::verify() {
    if (!holdsIDsOf<storage::EdgeIDType>(getInputEdges())) {
        return emitOpError("writes the properties of a column of edge IDs");
    } else if (!writesAMapPerRow(getValue())) {
        return emitOpError("writes a column of maps or of tagged cells, optionally nullable, or the null literal");
    } else if (!::db::namesPropertyElementTypes(getCreatedTypesAttr())) {
        return emitOpError("creates each property with the element type of a stored value");
    }

    return success();
}

LogicalResult ImportGraph::verify() {
    if (!::db::namesImportedPropertyTypes(getFormat(), getTypesAttr())) {
        return emitOpError("types each property as a datetime, duration or point a jsonl import reads, or a duration a parquet import reads");
    }

    return success();
}

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

// The pattern takes one argument per input column and the row tag, and ends naming what
// each match contributes to the list of the row it came from.
LogicalResult PatternComprehension::verify() {
    Block& patternBlock = getPattern().front();

    auto yield = dyn_cast_or_null<ComprehensionYield>(patternBlock.empty() ? nullptr : &patternBlock.back());
    if (!yield) {
        return emitOpError("pattern region must end with a db.comprehension_yield");
    }

    const mlir::OperandRange inputs = getInputColumns();
    const size_t inputCount = inputs.size();

    // No input column means no input row to tag: the one list covers the single empty row
    // the query starts from, which every match belongs to.
    const size_t expectedArguments = inputCount == 0 ? 0 : inputCount + 1;

    if (patternBlock.getNumArguments() != expectedArguments) {
        return emitOpError("pattern region takes one argument per input column plus the row tag, ")
               << "expected " << expectedArguments << " but has " << patternBlock.getNumArguments();
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        if (patternBlock.getArgument(inputIndex).getType() != inputs[inputIndex].getType()) {
            return emitOpError("pattern argument ") << inputIndex << " must have the type of input column "
                                                    << inputIndex;
        }
    }

    if (inputCount != 0) {
        const auto rowTagType =
            llvm::dyn_cast<ColumnType>(patternBlock.getArgument(inputCount).getType());

        if (!rowTagType || !rowTagType.getType().isUnsignedInteger(64)) {
            return emitOpError("pattern argument ") << inputCount << " must be the ui64 column of row tags";
        }
    }

    const bool tagsRows = inputCount != 0;
    const bool yieldsATag = yield.getRowTags() != nullptr;

    if (tagsRows != yieldsATag) {
        return emitOpError("the pattern yields a row tag exactly when the op takes input columns");
    }

    return success();
}

LogicalResult CreateVectorIndex::verify() {
    if (getDimension() == 0) {
        return emitOpError("must hold vectors of at least one dimension");
    }

    return success();
}

// The region's entry block stands for the rows in flight: one argument per input
// column, then the row tag. The db.optional_yield names the tag and the columns
// the region contributes - the input columns as the pattern left them, then the
// pattern's own variables - and the op's results are exactly those columns. A
// pattern variable is what a row the pattern missed comes back with null, so it
// has to be an ID column: those carry an invalid ID, which is how a null entity
// is spelled, where a property value column would need a null of its own.
LogicalResult OptionalMatch::verify() {
    Block& patternBlock = getPattern().front();

    auto yield = dyn_cast_or_null<OptionalYield>(patternBlock.empty() ? nullptr : &patternBlock.back());
    if (!yield) {
        return emitOpError("pattern region must end with a db.optional_yield");
    }

    const size_t inputCount = getInputColumns().size();

    // No input column means no input row to tag: the rows the pattern joins onto are
    // the single empty row the query starts from.
    const size_t expectedArguments = inputCount == 0 ? 0 : inputCount + 1;

    if (patternBlock.getNumArguments() != expectedArguments) {
        return emitOpError("pattern region takes one argument per input column plus the row tag, ")
               << "expected " << expectedArguments << " but has " << patternBlock.getNumArguments();
    }

    const bool tagsRows = inputCount != 0;
    const bool yieldsATag = yield.getTag() != nullptr;

    if (tagsRows != yieldsATag) {
        return emitOpError("the pattern yields a row tag exactly when the op takes input columns");
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        if (patternBlock.getArgument(inputIndex).getType() != getInputColumns()[inputIndex].getType()) {
            return emitOpError("pattern argument ") << inputIndex << " must have the type of input column "
                                                    << inputIndex;
        }
    }

    const ValueRange yieldedColumns = yield.getColumns();
    if (yieldedColumns.empty()) {
        return emitOpError("pattern must yield at least one column");
    }

    if (yieldedColumns.size() < inputCount) {
        return emitOpError("pattern must yield at least one column per input column, expected ")
               << inputCount << " but has " << yieldedColumns.size();
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        const mlir::Type inputType = getInputColumns()[inputIndex].getType();

        // A traversal rebinds its source to a typed ID column, so a pattern walking from a
        // column whose element type is still unspecified - what a CALL yields - hands it
        // back refined. Any other change of type is a carry set that lost track of a column.
        const auto inputColumn = llvm::dyn_cast<ColumnType>(inputType);
        const bool refinesAnUnspecifiedColumn = inputColumn
                                                && llvm::isa<mlir::NoneType>(inputColumn.getType());

        if (yieldedColumns[inputIndex].getType() != inputType && !refinesAnUnspecifiedColumn) {
            return emitOpError("yielded column ") << inputIndex << " must have the type of input column "
                                                  << inputIndex;
        }
    }

    for (size_t columnIndex = inputCount; columnIndex < yieldedColumns.size(); columnIndex++) {
        const auto column = llvm::dyn_cast<ColumnType>(yieldedColumns[columnIndex].getType());
        const mlir::Type elementType = column ? column.getType() : mlir::Type();

        // A function the pattern computes is typed during lowering, which pads the value it
        // resolves to
        const bool isPaddable = llvm::isa_and_present<storage::NodeIDType,
                                                      storage::EdgeIDType,
                                                      storage::ListType,
                                                      storage::PathType,
                                                      storage::NullableType,
                                                      mlir::NoneType>(elementType);

        if (!isPaddable) {
            return emitOpError("pattern variable column ")
                   << columnIndex << " must be a node or edge ID, list, path, nullable or function result column, "
                   << "so a missed match can be null";
        }
    }

    const Operation::result_type_range resultTypes = getOperation()->getResultTypes();
    if (resultTypes.size() != yieldedColumns.size()) {
        return emitOpError("expects ") << yieldedColumns.size()
                                       << " results, the columns the pattern yields, but has "
                                       << resultTypes.size();
    }

    for (size_t resultIndex = 0; resultIndex < resultTypes.size(); resultIndex++) {
        if (resultTypes[resultIndex] != yieldedColumns[resultIndex].getType()) {
            return emitOpError("result ") << resultIndex << " must be the yielded column type "
                                          << yieldedColumns[resultIndex].getType();
        }
    }

    return success();
}

LogicalResult ExistsSubquery::verify() {
    return verifySubqueryExpression<ExistsYield>(*this);
}

LogicalResult CountSubquery::verify() {
    return verifySubqueryExpression<CountSubqueryYield>(*this);
}

LogicalResult CollectSubquery::verify() {
    return verifySubqueryExpression<CollectSubqueryYield>(*this);
}

LogicalResult CallSubquery::verify() {
    Block& bodyBlock = getBody().front();

    auto yield = dyn_cast_or_null<SubqueryYield>(bodyBlock.empty() ? nullptr : &bodyBlock.back());
    if (!yield) {
        return emitOpError("body region must end with a db.subquery_yield");
    }

    const OperandRange inputs = getInputColumns();
    const size_t inputCount = inputs.size();

    // The body carries the row tag through its dataflow only when it carries the scope,
    // and only over rows it has: a body run one row at a time, or over the single empty
    // row, is tagged by the lowering instead.
    const bool tagsRows = getOptional() && getCarriesScope() && inputCount > 0;

    const size_t expectedArguments = tagsRows ? inputCount + 1 : inputCount;
    if (bodyBlock.getNumArguments() != expectedArguments) {
        return emitOpError("body region takes one argument per input column")
               << (tagsRows ? " plus the row tag" : "") << ", expected " << expectedArguments
               << " but has " << bodyBlock.getNumArguments();
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        if (bodyBlock.getArgument(inputIndex).getType() != inputs[inputIndex].getType()) {
            return emitOpError("body argument ") << inputIndex << " must have the type of input column "
                                                 << inputIndex;
        }
    }

    const bool yieldsATag = yield.getTag() != nullptr;
    if (tagsRows != yieldsATag) {
        return emitOpError("the body yields a row tag exactly when it takes one");
    }

    const bool readsArgumentsAsColumns = !getCarriesScope()
                                      && failed(verifyArgumentsReadAsRunConstants(getOperation(), bodyBlock));
    if (readsArgumentsAsColumns) {
        return failure();
    }

    const ValueRange yieldedColumns = yield.getColumns();
    const Operation::result_type_range resultTypes = getOperation()->getResultTypes();

    if (getUnit()) {
        if (getOptional()) {
            return emitOpError("a unit subquery yields no row for OPTIONAL to pad");
        }

        if (!yieldedColumns.empty()) {
            return emitOpError("a unit body yields no column");
        }

        if (resultTypes.size() != 0) {
            return emitOpError("a unit subquery has no result");
        }

        return success();
    }

    // The columns the results stand for: what the body yields when it carries the scope
    // itself, and the inputs ahead of what it yields otherwise.
    llvm::SmallVector<Type, 8> expectedResults;
    if (getCarriesScope()) {
        if (yieldedColumns.size() < inputCount) {
            return emitOpError("a body carrying its scope yields at least one column per input column, expected ")
                   << inputCount << " but has " << yieldedColumns.size();
        }

        for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
            const Type inputType = inputs[inputIndex].getType();

            // A traversal rebinds its source to a typed ID column, so a body walking from a
            // column whose element type is still unspecified hands it back refined.
            const auto inputColumn = llvm::dyn_cast<ColumnType>(inputType);
            const bool refinesAnUnspecifiedColumn = inputColumn && llvm::isa<mlir::NoneType>(inputColumn.getType());

            if (yieldedColumns[inputIndex].getType() != inputType && !refinesAnUnspecifiedColumn) {
                return emitOpError("yielded column ") << inputIndex << " must have the type of input column "
                                                      << inputIndex;
            }
        }
    } else {
        for (const Value input : inputs) {
            expectedResults.push_back(input.getType());
        }
    }

    for (const Value yielded : yieldedColumns) {
        expectedResults.push_back(yielded.getType());
    }

    if (resultTypes.size() != expectedResults.size()) {
        return emitOpError("expects ") << expectedResults.size()
                                       << " results, one per input column then one per yielded column, but has "
                                       << resultTypes.size();
    }

    for (size_t resultIndex = 0; resultIndex < expectedResults.size(); resultIndex++) {
        if (resultTypes[resultIndex] != expectedResults[resultIndex]) {
            return emitOpError("result ") << resultIndex << " must have type " << expectedResults[resultIndex];
        }
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

bool PatternComprehension::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    carrySet = RegionCarrySet {._count = getInputColumns().size()};
    return true;
}

bool OptionalMatch::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    carrySet = RegionCarrySet {._count = getInputColumns().size(), ._handsBack = true, ._yieldsBack = true};
    return true;
}

void OptionalMatch::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    OptionalYield yield = cast<OptionalYield>(getPattern().front().getTerminator());

    collectCarriedResult(value, 0, yield.getColumns(), carried);
    collectCarriedArgument(value, getInputColumns(), carried);
}

// A body carrying its scope yields the inputs back ahead of its own columns; otherwise the
// inputs come back unchanged ahead of what the body yields
void CallSubquery::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    SubqueryYield yield = cast<SubqueryYield>(getBody().front().getTerminator());
    const OperandRange inputs = getInputColumns();

    if (getCarriesScope()) {
        collectCarriedResult(value, 0, yield.getColumns(), carried);
    } else {
        collectCarriedResult(value, 0, inputs, carried);
        collectCarriedResult(value, inputs.size(), yield.getColumns(), carried);
    }

    collectCarriedArgument(value, inputs, carried);
}

bool CallSubquery::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    const bool handsBack = !getUnit();
    carrySet = RegionCarrySet {._count = getInputColumns().size(),
                               ._handsBack = handsBack,
                               ._yieldsBack = handsBack && getCarriesScope()};
    return true;
}

// A row the body missed comes out of an optional call with what it yields null
bool CallSubquery::getYieldedResultOffset(Region& region, size_t& resultOffset) {
    if (getUnit() || getOptional()) {
        return false;
    }

    resultOffset = getCarriesScope() ? 0 : getInputColumns().size();
    return true;
}

void ExistsSubquery::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    collectCarriedArgument(value, getInputColumns(), carried);
}

bool ExistsSubquery::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    carrySet = RegionCarrySet {._count = getInputColumns().size()};
    return true;
}

void CountSubquery::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    collectCarriedArgument(value, getInputColumns(), carried);
}

bool CountSubquery::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    carrySet = RegionCarrySet {._count = getInputColumns().size()};
    return true;
}

void CollectSubquery::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    collectCarriedArgument(value, getInputColumns(), carried);
}

bool CollectSubquery::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    carrySet = RegionCarrySet {._count = getInputColumns().size()};
    return true;
}
