#include "DBOps.h"
#include "DBDialect.h"

#include <optional>

#include "llvm/ADT/SmallVector.h"

#include "IRLiteralList.h"
#include "StorageEnums.h"
#include "ColumnIndicesFormat.h"
#include "GroupAggregateKindsFormat.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

#define GET_OP_CLASSES
#include "DBOps.cpp.inc"

namespace {

// The keyword that introduces each factor region in the textual form
// `db.cross_product factor { ... } factor { ... }`.
const char* const factorKeyword = "factor";

// The db.yield that terminates a factor region, or a null Yield if the region
// is empty or does not end with one. A factor's yield names the columns that
// factor contributes to the product.
Yield getFactorYield(Region& factor) {
    Operation* terminator = nullptr;
    if (!factor.empty()) {
        Block& block = factor.front();
        if (!block.empty()) {
            terminator = &block.back();
        }
    }

    return dyn_cast_or_null<Yield>(terminator);
}

// Appends the columns yielded by a factor region to resultTypes, failing with a
// diagnostic if the factor is not terminated by a db.yield.
ParseResult appendFactorYieldTypes(OpAsmParser& parser,
                                   Region& factor,
                                   llvm::SmallVectorImpl<Type>& resultTypes) {
    Yield yield = getFactorYield(factor);
    if (!yield) {
        return parser.emitError(parser.getCurrentLocation(),
                                "cross_product factor must end with a db.yield");
    }

    for (const Type columnType : yield.getColumns().getTypes()) {
        resultTypes.push_back(columnType);
    }

    return success();
}

// Parses `factor { ... }` into a fresh region of result. The factor takes no
// operands and no block arguments, so the region is parsed with an empty
// argument list.
ParseResult parseFactorRegion(OpAsmParser& parser, OperationState& result) {
    if (parser.parseKeyword(factorKeyword)) {
        return failure();
    }

    Region* factor = result.addRegion();
    return parser.parseRegion(*factor, {});
}

// A literal list typed as homogeneous - db.unwind_const's typed column, db.const_list's
// typed list - must carry at least one element and every element must be a typed
// attribute of one shared type. The elements are checked against each other and not
// against the spelled element type: a hand-written "s" parses as an untyped StringAttr,
// so comparing it to a !storage.string would reject valid IR. A homogeneous result paired
// with literals of another type is therefore not an op-level error; the runtime fill
// catches that on the element's type tag.
LogicalResult verifyHomogeneousElements(Operation* op, ArrayAttr elements) {
    if (elements.empty()) {
        return op->emitOpError("a homogeneous literal list must carry at least one element; "
                               "an empty list is the list_element form");
    }

    const TypedAttr firstElement = llvm::dyn_cast<TypedAttr>(elements[0]);
    if (!firstElement) {
        return op->emitOpError("literal list element is not a typed attribute");
    }

    const mlir::Type payloadType = firstElement.getType();
    for (const Attribute element : elements) {
        const TypedAttr typedElement = llvm::dyn_cast<TypedAttr>(element);
        if (!typedElement) {
            return op->emitOpError("literal list element is not a typed attribute");
        } else if (typedElement.getType() != payloadType) {
            return op->emitOpError("a homogeneous literal list requires every element to share one type");
        }
    }

    return success();
}

// db.limit and db.skip both pass their columns straight through, so the results
// must be exactly the input columns - same count, same types and in the same
// order. Their row count is read from the first column during lowering, so a
// pass-through over no column cannot be sized and is rejected here at the db
// level. Shared by both verifiers.
LogicalResult verifyPassThrough(Operation* op,
                                OperandRange columns,
                                Operation::result_range results) {
    if (columns.empty()) {
        return op->emitOpError("requires at least one column");
    }

    if (columns.size() != results.size()) {
        return op->emitOpError("expects ") << columns.size()
                                           << " results, one per input column, but has "
                                           << results.size();
    }

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (columns[columnIndex].getType() != results[columnIndex].getType()) {
            return op->emitOpError("result ") << columnIndex
                                              << " must have the same type as input column "
                                              << columnIndex;
        }
    }

    return success();
}

}

// Ensures each variable has a numeric name
void ScanEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

// Ensures each variable has a numeric name
void GetOutEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetInEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetOutEdgesByType::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetInEdgesByType::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

// Builds the op from just the result types - the left factor's yielded columns
// followed by the right factor's - and creates the two empty factor blocks. The
// caller fills each region and terminates it with a db.yield whose operands
// match the result types contributed by that factor. The insertion guard keeps
// the block creation from leaking out of the builder.
void CrossProduct::build(OpBuilder& builder, OperationState& state, TypeRange resultTypes) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addTypes(resultTypes);

    Region* leftFactor = state.addRegion();
    builder.createBlock(leftFactor);

    Region* rightFactor = state.addRegion();
    builder.createBlock(rightFactor);
}

// Custom syntax, mirroring the disconnected pattern it models:
//
//   %a, %b = db.cross_product factor { ... db.yield %x : ... }
//                             factor { ... db.yield %y : ... }
//
// Nothing is spelled after the regions: the result types are recovered from the
// two factors' yields - the left factor's yielded columns followed by the
// right factor's - matching the result count parsed from the `%a, %b =` list.
ParseResult CrossProduct::parse(OpAsmParser& parser, OperationState& result) {
    const bool regionsFailed = parseFactorRegion(parser, result)
                               || parseFactorRegion(parser, result)
                               || parser.parseOptionalAttrDict(result.attributes);
    if (regionsFailed) {
        return failure();
    }

    Region& leftFactor = *result.regions[0];
    Region& rightFactor = *result.regions[1];
    if (appendFactorYieldTypes(parser, leftFactor, result.types)
        || appendFactorYieldTypes(parser, rightFactor, result.types)) {
        return failure();
    }

    return success();
}

void CrossProduct::print(OpAsmPrinter& printer) {
    printer << " " << factorKeyword << " ";
    printer.printRegion(getLeftFactor());

    printer << " " << factorKeyword << " ";
    printer.printRegion(getRightFactor());

    printer.printOptionalAttrDict((*this)->getAttrs());
}

// The results must be exactly the columns yielded by the two factors: the left
// factor's yielded columns followed by the right factor's. The yields drive the
// result types during parsing, so this guards the programmatic builder path and
// re-checks parsed IR.
LogicalResult CrossProduct::verify() {
    Yield leftYield = getFactorYield(getLeftFactor());
    Yield rightYield = getFactorYield(getRightFactor());
    if (!leftYield || !rightYield) {
        return emitOpError("each factor region must end with a db.yield");
    }

    // Each factor must contribute at least one column. A side's row count is read
    // from its first yielded column during lowering, so a factor that surfaces no
    // column (an empty db.yield) cannot be sized - reject it here at the db level.
    if (leftYield.getColumns().empty() || rightYield.getColumns().empty()) {
        return emitOpError("each factor must yield at least one column");
    }

    llvm::SmallVector<Type> expectedResultTypes;
    for (const Type columnType : leftYield.getColumns().getTypes()) {
        expectedResultTypes.push_back(columnType);
    }
    for (const Type columnType : rightYield.getColumns().getTypes()) {
        expectedResultTypes.push_back(columnType);
    }

    const Operation::result_type_range resultTypes = getOperation()->getResultTypes();
    if (resultTypes.size() != expectedResultTypes.size()) {
        return emitOpError("expects ") << expectedResultTypes.size()
                                       << " results, the columns yielded by the two factors, but has "
                                       << resultTypes.size();
    }

    for (size_t resultIndex = 0; resultIndex < expectedResultTypes.size(); resultIndex++) {
        if (resultTypes[resultIndex] != expectedResultTypes[resultIndex]) {
            return emitOpError("result ") << resultIndex << " must be the yielded column type "
                                          << expectedResultTypes[resultIndex];
        }
    }

    return success();
}

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
    if (getEdgeType().empty()) {
        return emitOpError("requires a non-empty edge type");
    }

    if (getPropNames().size() != getPropValues().size()) {
        return emitOpError("prop_names and prop_values must have the same count, but has ")
               << getPropNames().size() << " names and " << getPropValues().size() << " values";
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

// A label scan must name at least one label to filter by; a label-free scan of
// every node is db.scan_nodes, so an empty label list is malformed IR here.
LogicalResult ScanNodesByLabel::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult CheckLabelConstraint::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult CheckEdgeTypeConstraint::verify() {
    if (getEdgeTypes().empty()) {
        return emitOpError("requires at least one edge type");
    }

    return success();
}

void Output::build(OpBuilder& builder, OperationState& state, ValueRange columns) {
    Output::build(builder, state, columns, ArrayAttr());
}

LogicalResult Output::verify() {
    const ArrayAttr columnNames = getColumnNamesAttr();
    if (columnNames && columnNames.size() != getColumns().size()) {
        return emitOpError("names must give one name per output column, but has ")
               << columnNames.size() << " names for " << getColumns().size() << " columns";
    }

    return success();
}

// db.limit passes its columns straight through, so the results must be exactly
// the input columns - same count, same types and in the same order.
LogicalResult Limit::verify() {
    return verifyPassThrough(getOperation(), getColumns(), getResults());
}

// db.skip passes its columns straight through, the same as db.limit.
LogicalResult Skip::verify() {
    return verifyPassThrough(getOperation(), getColumns(), getResults());
}

// db.sort passes its columns straight through reordered, so the results must be
// exactly the input columns - same count, same types, same order. The two key
// arrays must be parallel and every key must index a column that exists.
LogicalResult Sort::verify() {
    const OperandRange columns = getColumns();
    const Operation::result_range results = getResults();

    // A sort with no column has nothing to reorder, and its row count would have
    // no column to be read from during lowering. Reject it here at the db level.
    if (columns.empty()) {
        return emitOpError("requires at least one column");
    }

    if (columns.size() != results.size()) {
        return emitOpError("expects ") << columns.size()
                                       << " results, one per input column, but has "
                                       << results.size();
    }

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (columns[columnIndex].getType() != results[columnIndex].getType()) {
            return emitOpError("result ") << columnIndex
                                          << " must have the same type as input column "
                                          << columnIndex;
        }
    }

    const ArrayRef<int64_t> keyColumns = getKeyColumns();
    const ArrayRef<bool> keyAscending = getKeyAscending();

    // At least one key, and one direction per key: the two arrays describe the
    // same list of sort keys, so a mismatch is malformed IR.
    if (keyColumns.empty()) {
        return emitOpError("requires at least one sort key");
    }

    if (keyColumns.size() != keyAscending.size()) {
        return emitOpError("expects one ascending flag per key, but has ")
               << keyColumns.size() << " keys and " << keyAscending.size() << " flags";
    }

    // Every key names a column to sort by, so it must index one of the columns.
    for (const int64_t keyColumn : keyColumns) {
        const bool inRange = keyColumn >= 0 && static_cast<size_t>(keyColumn) < columns.size();
        if (!inRange) {
            return emitOpError("sort key ") << keyColumn << " is out of range for "
                                            << columns.size() << " columns";
        }
    }

    return success();
}

// db.remove_duplicates passes its columns straight through (minus duplicate rows),
// so the results must be exactly the input columns - same count, same types, same
// order - the shared pass-through check db.limit and db.skip use. The dedup key is
// the whole row, so - unlike db.sort - there are no key arrays to validate.
LogicalResult RemoveDuplicates::verify() {
    return verifyPassThrough(getOperation(), getColumns(), getResults());
}

// A call produces one column per yielded return value then one per carried column, so
// `yields`, the carry set and the results must line up. A call yielding nothing has no
// column to produce and no row count to be read from, so it is rejected here at the db
// level; the procedure's return values themselves are checked against the registry
// during lowering, which is where the registry is available.
LogicalResult CallProcedure::verify() {
    const ArrayAttr yields = getYields();
    const OperandRange carriedColumns = getCarriedColumns();

    // A call yielding nothing produces no column, so it has no row count for a carried
    // row to be replicated against - it is called for what it does, not for rows, and
    // nothing can ride through it.
    if (yields.empty() && !carriedColumns.empty()) {
        return emitOpError("yields no return value, so it cannot carry ")
               << carriedColumns.size() << " columns past it";
    }

    const Operation::result_range results = getResults();
    const size_t expectedResults = yields.size() + carriedColumns.size();
    if (results.size() != expectedResults) {
        return emitOpError("expects ") << expectedResults
                                       << " results, one per yielded return value and carried column, but has "
                                       << results.size();
    }

    // A carried column is only replicated by the call, never retyped, so each trailing
    // result keeps its carried column's type - the pass-through check db.limit shares,
    // applied to the results after the yields.
    for (size_t carriedIndex = 0; carriedIndex < carriedColumns.size(); carriedIndex++) {
        const Value carried = carriedColumns[carriedIndex];
        const Value result = results[yields.size() + carriedIndex];

        if (carried.getType() != result.getType()) {
            return emitOpError("carried result ") << carriedIndex
                                                  << " must have the same type as carried column "
                                                  << carriedIndex;
        }
    }

    return success();
}

// Allows inline declaration of a constant type
LogicalResult ConstantOp::inferReturnTypes(MLIRContext* context,
                                           std::optional<Location> location,
                                           ValueRange operands, DictionaryAttr attributes,
                                           PropertyRef properties,
                                           RegionRange regions,
                                           SmallVectorImpl<Type>& inferredReturnTypes) {
    ConstantOpGenericAdaptor adaptor(operands, attributes, properties, regions);

    // An array of per-element attributes is a list literal; it carries no type of its own,
    // so the element type is the homogeneity verdict over the elements.
    if (const auto elements = llvm::dyn_cast<ArrayAttr>(adaptor.getValue())) {
        const mlir::Type shared = ::db::sharedLiteralElementType(elements);
        const mlir::Type listElement = shared ? shared : storage::ListElementType::get(context);
        const mlir::Type listType = storage::ListType::get(context, listElement);

        inferredReturnTypes.emplace_back(mlir::db::ColumnType::get(context, listType));
        return success();
    }

    // A dense f32 array is an embedding: a flat run of floats, which is the one attribute
    // kind carrying no type of its own that still names its column's element type.
    if (llvm::isa<mlir::DenseF32ArrayAttr>(adaptor.getValue())) {
        const mlir::Type embeddingType = storage::EmbeddingType::get(context);

        inferredReturnTypes.emplace_back(mlir::db::ColumnType::get(context, embeddingType));
        return success();
    }

    // Type inference runs while the op is still being parsed, ahead of the operand
    // constraint that limits the value to those three kinds, so an attribute of any other
    // kind has to be turned away here rather than cast blindly.
    const mlir::TypedAttr typedValue = llvm::dyn_cast<mlir::TypedAttr>(adaptor.getValue());
    if (!typedValue) {
        return mlir::emitOptionalError(location,
                                       "db.constant carries a typed value, a dense f32 array "
                                       "or an array of literals, not ", adaptor.getValue());
    }

    inferredReturnTypes.emplace_back(mlir::db::ColumnType::get(context, typedValue.getType()));
    return mlir::success();
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

// A carried column comes back replicated, never retyped, so the carry set and the
// results past the element column must match one for one.
LogicalResult Unwind::verify() {
    const OperandRange carriedColumns = getColumnsToFilter();
    const Operation::result_range carriedResults = getCarried();

    if (carriedColumns.size() != carriedResults.size()) {
        return emitOpError("expects one carried result per carried column, but has ")
               << carriedResults.size() << " for " << carriedColumns.size();
    }

    for (size_t carriedIndex = 0; carriedIndex < carriedColumns.size(); carriedIndex++) {
        if (carriedColumns[carriedIndex].getType() != carriedResults[carriedIndex].getType()) {
            return emitOpError("carried result ") << carriedIndex
                                                  << " must have the same type as carried column "
                                                  << carriedIndex;
        }
    }

    return success();
}

// The unwound column's element type is the homogeneity verdict: a type-erased
// list_element column accepts any elements, a typed one requires them to share that one
// type - the shared check the const_list verifier runs too.
LogicalResult UnwindConst::verify() {
    const ColumnType resultColumn = llvm::dyn_cast<ColumnType>(getResult().getType());
    if (!resultColumn) {
        return emitOpError("result must be a column");
    }

    const mlir::Type elementType = resultColumn.getType();
    if (llvm::isa<storage::ListElementType>(elementType)) {
        return success();
    }

    return verifyHomogeneousElements(getOperation(), getElements());
}

// One column per field, and a field is named either by position or by header - a
// header only where a header line was read, since nothing else resolves the name.
LogicalResult LoadCSV::verify() {
    const ArrayAttr fields = getFieldsAttr();

    // A load producing no column produces no rows either: nothing downstream could read
    // the records it opened, and its loop would have no chunk to size a step from
    if (fields.empty()) {
        return emitOpError("requires at least one field");
    }

    if (fields.size() != getResults().size()) {
        return emitOpError("produces one column per field, but names ")
               << fields.size() << " fields for " << getResults().size() << " columns";
    }

    for (const Attribute field : fields) {
        if (const auto header = llvm::dyn_cast<StringAttr>(field)) {
            if (!getWithHeaders()) {
                return emitOpError("names field '") << header.getValue()
                                                    << "' by header, which only a with_headers load resolves";
            }
        } else {
            const auto index = llvm::dyn_cast<IntegerAttr>(field);
            if (!index || !index.getType().isUnsignedInteger(64)) {
                return emitOpError("field must be a ui64 position or a header name");
            }
        }
    }

    return success();
}

// A search reporting no neighbour, or searching for no vector, is a query that asked for
// nothing: neither is a shape the index can be asked for, so both are rejected here rather
// than left to return an empty result the query would read as "no match".
LogicalResult VectorSearch::verify() {
    if (getK() == 0) {
        return emitOpError("must report at least one neighbour");
    }

    if (getQueryVector().empty()) {
        return emitOpError("must search for a vector of at least one dimension");
    }

    return success();
}

