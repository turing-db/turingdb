#include "DBOps.h"
#include "DBDialect.h"

#include "llvm/ADT/SmallVector.h"

#include "StorageEnums.h"
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

// Allows inline declaration of a constant type
LogicalResult ConstantOp::inferReturnTypes(MLIRContext* context,
                                           std::optional<Location> location,
                                           ValueRange operands, DictionaryAttr attributes,
                                           PropertyRef properties,
                                           RegionRange regions,
                                           SmallVectorImpl<Type>& inferredReturnTypes) {
    ConstantOpGenericAdaptor adaptor(operands, attributes, properties, regions);
    const mlir::TypedAttr typedValue = mlir::cast<mlir::TypedAttr>(adaptor.getValue());

    mlir::Type elementType;
    if (const auto elements = mlir::dyn_cast<mlir::DenseElementsAttr>(typedValue)) {
        const auto shapedType = mlir::cast<mlir::ShapedType>(elements.getType());
        const bool isEmbedding = shapedType.getElementType().isF32()
                                 && shapedType.hasRank()
                                 && shapedType.getRank() == 1;
        elementType = isEmbedding ? storage::EmbeddingType::get(context) : typedValue.getType();
    } else {
        elementType = typedValue.getType();
    }

    inferredReturnTypes.emplace_back(mlir::db::ColumnType::get(context, elementType));
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
    // GroupAggregateKind (count / sum / min / max / avg).
    for (const int64_t kind : kinds) {
        if (!storage::symbolizeGroupAggregateKind(kind)) {
            return emitOpError("has an unknown aggregate kind ") << kind;
        }
    }

    return success();
}
