#include "DBOps.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "DBRegionColumns.h"

using namespace mlir;
using namespace mlir::db;

namespace {

Value yieldedFactorColumn(Region& leftFactor, Region& rightFactor, size_t resultNumber) {
    Yield leftYield = cast<Yield>(leftFactor.front().getTerminator());
    const OperandRange leftColumns = leftYield.getColumns();
    if (resultNumber < leftColumns.size()) {
        return leftColumns[resultNumber];
    }

    Yield rightYield = cast<Yield>(rightFactor.front().getTerminator());

    return rightYield.getColumns()[resultNumber - leftColumns.size()];
}

size_t factorResultOffset(Region& leftFactor, Region& factor) {
    if (&factor == &leftFactor) {
        return 0;
    }

    Yield leftYield = cast<Yield>(leftFactor.front().getTerminator());

    return leftYield.getColumns().size();
}

// The keyword that introduces each factor region in the textual form
// `db.cross_product factor { ... } factor { ... }`.
const char* const factorKeyword = "factor";

// The keyword db.hash_join spells its two key column indices after.
const char* const keysKeyword = "on";

// Appends the columns yielded by a factor region to resultTypes, failing with a
// diagnostic if the factor is not terminated by a db.yield.
ParseResult appendFactorYieldTypes(OpAsmParser& parser,
                                   Region& factor,
                                   llvm::SmallVectorImpl<Type>& resultTypes) {
    Yield yield = getFactorYield(factor);
    if (!yield) {
        return parser.emitError(parser.getCurrentLocation(),
                                "factor must end with a db.yield");
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

// The results of a two-factor op - db.cross_product, db.hash_join - must be exactly the
// columns the two factors yield: the left factor's yielded columns followed by the right
// factor's. The yields drive the result types during parsing, so this guards the
// programmatic builder path and re-checks parsed IR. Shared by both verifiers.
LogicalResult verifyFactorResults(Operation* op, Region& leftFactor, Region& rightFactor) {
    Yield leftYield = getFactorYield(leftFactor);
    Yield rightYield = getFactorYield(rightFactor);
    if (!leftYield || !rightYield) {
        return op->emitOpError("each factor region must end with a db.yield");
    }

    // Each factor must contribute at least one column. A side's row count is read
    // from its first yielded column during lowering, so a factor that surfaces no
    // column (an empty db.yield) cannot be sized - reject it here at the db level.
    if (leftYield.getColumns().empty() || rightYield.getColumns().empty()) {
        return op->emitOpError("each factor must yield at least one column");
    }

    llvm::SmallVector<Type> expectedResultTypes;
    for (const Type columnType : leftYield.getColumns().getTypes()) {
        expectedResultTypes.push_back(columnType);
    }
    for (const Type columnType : rightYield.getColumns().getTypes()) {
        expectedResultTypes.push_back(columnType);
    }

    const Operation::result_type_range resultTypes = op->getResultTypes();
    if (resultTypes.size() != expectedResultTypes.size()) {
        return op->emitOpError("expects ") << expectedResultTypes.size()
                                           << " results, the columns yielded by the two factors, but has "
                                           << resultTypes.size();
    }

    for (size_t resultIndex = 0; resultIndex < expectedResultTypes.size(); resultIndex++) {
        if (resultTypes[resultIndex] != expectedResultTypes[resultIndex]) {
            return op->emitOpError("result ") << resultIndex << " must be the yielded column type "
                                              << expectedResultTypes[resultIndex];
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

LogicalResult CrossProduct::verify() {
    return verifyFactorResults(getOperation(), getLeftFactor(), getRightFactor());
}

// Builds the op from the result types - the left factor's yielded columns followed
// by the right factor's - and the two key column indices, and creates the two empty
// factor blocks. The cross product's sibling builder, with the join keys added.
void HashJoin::build(OpBuilder& builder,
                     OperationState& state,
                     TypeRange resultTypes,
                     uint64_t leftKey,
                     uint64_t rightKey) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addTypes(resultTypes);

    const Type keyIndexType = builder.getIntegerType(64, /*isSigned=*/false);
    state.addAttribute(getLeftKeyAttrName(state.name), builder.getIntegerAttr(keyIndexType, leftKey));
    state.addAttribute(getRightKeyAttrName(state.name), builder.getIntegerAttr(keyIndexType, rightKey));

    Region* leftFactor = state.addRegion();
    builder.createBlock(leftFactor);

    Region* rightFactor = state.addRegion();
    builder.createBlock(rightFactor);
}

// Custom syntax, the cross product's with the join keys spelled after the regions:
//
//   %a, %ka, %b, %kb = db.hash_join factor { ... db.yield %x, %k : ... }
//                                   factor { ... db.yield %y, %k : ... } on 1, 1
//
// The result types are recovered from the two factors' yields, exactly as
// db.cross_product recovers them.
ParseResult HashJoin::parse(OpAsmParser& parser, OperationState& result) {
    if (parseFactorRegion(parser, result) || parseFactorRegion(parser, result)) {
        return failure();
    }

    uint64_t leftKey = 0;
    uint64_t rightKey = 0;
    const bool keysFailed = parser.parseKeyword(keysKeyword)
                            || parser.parseInteger(leftKey)
                            || parser.parseComma()
                            || parser.parseInteger(rightKey);
    if (keysFailed) {
        return failure();
    }

    if (parser.parseOptionalAttrDict(result.attributes)) {
        return failure();
    }

    const Type keyIndexType = parser.getBuilder().getIntegerType(64, /*isSigned=*/false);
    result.addAttribute(getLeftKeyAttrName(result.name), IntegerAttr::get(keyIndexType, leftKey));
    result.addAttribute(getRightKeyAttrName(result.name), IntegerAttr::get(keyIndexType, rightKey));

    Region& leftFactor = *result.regions[0];
    Region& rightFactor = *result.regions[1];
    if (appendFactorYieldTypes(parser, leftFactor, result.types)
        || appendFactorYieldTypes(parser, rightFactor, result.types)) {
        return failure();
    }

    return success();
}

void HashJoin::print(OpAsmPrinter& printer) {
    printer << " " << factorKeyword << " ";
    printer.printRegion(getLeftFactor());

    printer << " " << factorKeyword << " ";
    printer.printRegion(getRightFactor());

    printer << " " << keysKeyword << " " << getLeftKey() << ", " << getRightKey();

    printer.printOptionalAttrDict((*this)->getAttrs(), {getLeftKeyAttrName(), getRightKeyAttrName()});
}

// The results line up with the two factors' yields exactly as a cross product's do, and
// each key index has to name a column of its own factor's yield - it is what the join
// matches on, so a key past the end names no column to read.
LogicalResult HashJoin::verify() {
    if (failed(verifyFactorResults(getOperation(), getLeftFactor(), getRightFactor()))) {
        return failure();
    }

    const size_t leftCount = getFactorYield(getLeftFactor()).getColumns().size();
    const size_t rightCount = getFactorYield(getRightFactor()).getColumns().size();

    if (getLeftKey() >= leftCount) {
        return emitOpError("left key column ") << getLeftKey() << " is past the "
                                               << leftCount << " columns the left factor yields";
    }

    if (getRightKey() >= rightCount) {
        return emitOpError("right key column ") << getRightKey() << " is past the "
                                                << rightCount << " columns the right factor yields";
    }

    return success();
}

Value CrossProduct::getRowSource(OpResult result) {
    return yieldedFactorColumn(getLeftFactor(), getRightFactor(), result.getResultNumber());
}

bool CrossProduct::getYieldedResultOffset(Region& region, size_t& resultOffset) {
    resultOffset = factorResultOffset(getLeftFactor(), region);
    return true;
}

Value HashJoin::getRowSource(OpResult result) {
    return yieldedFactorColumn(getLeftFactor(), getRightFactor(), result.getResultNumber());
}

bool HashJoin::getYieldedResultOffset(Region& region, size_t& resultOffset) {
    resultOffset = factorResultOffset(getLeftFactor(), region);
    return true;
}
