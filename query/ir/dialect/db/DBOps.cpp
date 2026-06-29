#include "DBOps.h"
#include "DBDialect.h"

#include "llvm/ADT/SmallVector.h"

using namespace mlir;
using namespace mlir::db;

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

// db.limit passes its columns straight through, so the results must be exactly
// the input columns - same count, same types and in the same order.
LogicalResult Limit::verify() {
    return verifyPassThrough(getOperation(), getColumns(), getResults());
}

// db.skip passes its columns straight through, the same as db.limit.
LogicalResult Skip::verify() {
    return verifyPassThrough(getOperation(), getColumns(), getResults());
}
