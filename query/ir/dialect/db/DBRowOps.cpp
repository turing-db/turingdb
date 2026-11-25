#include "DBOps.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "DBColumnChecks.h"
#include "DBRegionColumns.h"
#include "IRConstantColumn.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

namespace {

void collectBranchYields(Value value, MutableArrayRef<Region> branches, llvm::SmallVectorImpl<Value>& carried) {
    const OpResult result = dyn_cast<OpResult>(value);
    if (!result) {
        return;
    }

    for (Region& branch : branches) {
        Yield yield = cast<Yield>(branch.front().getTerminator());
        carried.push_back(yield.getColumns()[result.getResultNumber()]);
    }
}

// The db.output a union branch ends with, or a null Output when the branch is empty or
// ends on something else. A branch's output is what fills the union's result table.
Output getBranchOutput(Region& branch) {
    Operation* last = nullptr;
    if (!branch.empty()) {
        Block& block = branch.front();
        if (!block.empty()) {
            last = &block.back();
        }
    }

    return dyn_cast_or_null<Output>(last);
}

// A branch feeding the results of the op holding it, rather than the result table, ends
// in a db.yield naming one column per result
LogicalResult verifyYieldingBranch(Operation* holder, Region& branch) {
    Yield yield = getFactorYield(branch);
    if (!yield) {
        return holder->emitOpError("each branch must end with a db.yield");
    }

    if (yield.getColumns().size() != holder->getNumResults()) {
        return holder->emitOpError("each branch must yield one column per result");
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

// The count of a db.limit or a db.skip: the literal, or an integer bound for the whole run, or
// a cell holding one, which the loop checks when it starts
LogicalResult verifyCutCount(Operation* op, std::optional<uint64_t> count, Value boundCount) {
    if (count.has_value() == static_cast<bool>(boundCount)) {
        return op->emitOpError("requires either a count or a bound one");
    } else if (!boundCount) {
        return success();
    }

    const Type element = columnElement(boundCount);
    if (!element.isSignlessInteger(64) && !isa<NoneType, storage::AnyType>(element)) {
        return op->emitOpError("requires a bound count of type i64 or !storage.any, got ") << element;
    } else if (!::db::readsNoRow(boundCount)) {
        return op->emitOpError("requires a bound count computed from parameters and literals alone");
    }

    return success();
}

}

// Builds the op from the branch count alone and creates that many empty blocks. The
// caller fills each region with a query body and ends it with a db.output. The insertion
// guard keeps the block creation from leaking out of the builder, as CrossProduct's does.
void Union::build(OpBuilder& builder, OperationState& state, size_t branchCount) {
    build(builder, state, TypeRange {}, branchCount);
}

void Union::build(OpBuilder& builder, OperationState& state, TypeRange resultTypes, size_t branchCount) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addTypes(resultTypes);

    for (size_t branchIndex = 0; branchIndex < branchCount; branchIndex++) {
        Region* branch = state.addRegion();
        builder.createBlock(branch);
    }
}

// Custom syntax, one region per branch separated by commas:
//
//   db.union { ... db.output(%a) names ["name"] : ... },
//             { ... db.output(%b) names ["name"] : ... }
//
// A union with results spells their types after the regions:
//
//   %z = db.union { ... db.yield %a : ... }, { ... db.yield %b : ... } : !db.column<none>
ParseResult Union::parse(OpAsmParser& parser, OperationState& result) {
    do {
        Region* branch = result.addRegion();
        if (parser.parseRegion(*branch, {})) {
            return failure();
        }
    } while (succeeded(parser.parseOptionalComma()));

    if (succeeded(parser.parseOptionalColon())) {
        SmallVector<Type> resultTypes;
        if (parser.parseTypeList(resultTypes)) {
            return failure();
        }

        result.addTypes(resultTypes);
    }

    return parser.parseOptionalAttrDict(result.attributes);
}

void Union::print(OpAsmPrinter& printer) {
    llvm::interleave(getBranches(),
                     printer,
                     [&](Region& branch) {
                         printer << " ";
                         printer.printRegion(branch);
                     },
                     ",");

    const ResultRange results = getResults();
    if (!results.empty()) {
        printer << " : ";
        llvm::interleaveComma(results.getTypes(), printer);
    }

    printer.printOptionalAttrDict((*this)->getAttrs());
}

LogicalResult Materialize::verify() {
    if (getColumns().empty()) {
        return emitOpError("requires at least one column");
    }

    return success();
}

LogicalResult ScanMaterialized::verify() {
    Materialize materialize = getRows().getDefiningOp<Materialize>();
    if (!materialize) {
        return emitOpError("requires rows a db.materialize holds");
    }

    const OperandRange heldColumns = materialize.getColumns();
    const ResultRange columns = getColumns();
    if (columns.size() != heldColumns.size()) {
        return emitOpError("expects ") << heldColumns.size()
                                       << " results, one per column the db.materialize holds, but has "
                                       << columns.size();
    }

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (columns[columnIndex].getType() != heldColumns[columnIndex].getType()) {
            return emitOpError("result ") << columnIndex << " must have the type of the column it reads";
        }
    }

    return success();
}

// A union emits one result table, so every branch has to end in a db.output and all of
// them have to name the same columns. The names are the query's, checked by the analyzer;
// what is re-checked here is the shape a hand-written module can still get wrong.
LogicalResult Union::verify() {
    const MutableArrayRef<Region> branches = getBranches();
    if (branches.size() < 2) {
        return emitOpError("requires at least two branches");
    }

    if (getNumResults() > 0) {
        for (Region& branch : branches) {
            if (failed(verifyYieldingBranch(getOperation(), branch))) {
                return failure();
            }
        }

        return success();
    }

    Output first;
    for (Region& branch : branches) {
        Output output = getBranchOutput(branch);
        if (!output) {
            return emitOpError("each branch must end with a db.output");
        }

        if (!first) {
            first = output;
            continue;
        }

        if (output.getColumns().size() != first.getColumns().size()) {
            return emitOpError("every branch must output the same number of columns");
        }

        if (output.getColumnNamesAttr() != first.getColumnNamesAttr()) {
            return emitOpError("every branch must output the same column names");
        }
    }

    return success();
}

void Conditional::build(OpBuilder& builder,
                        OperationState& state,
                        TypeRange resultTypes,
                        size_t regionCount) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addTypes(resultTypes);

    for (size_t regionIndex = 0; regionIndex < regionCount; regionIndex++) {
        Region* region = state.addRegion();
        builder.createBlock(region);
    }
}

LogicalResult Conditional::verify() {
    const MutableArrayRef<Region> regions = getBranches();
    if (regions.size() < 2) {
        return emitOpError("requires a condition and the branch it guards");
    }

    const size_t conditionCount = regions.size() / 2;
    for (size_t conditionIndex = 0; conditionIndex < conditionCount; conditionIndex++) {
        Yield condition = getFactorYield(regions[2 * conditionIndex]);
        if (!condition || condition.getColumns().size() != 1) {
            return emitOpError("each condition must end with a db.yield of one column");
        }

        if (failed(verifyYieldingBranch(getOperation(), regions[2 * conditionIndex + 1]))) {
            return failure();
        }
    }

    const bool hasDefault = regions.size() % 2 == 1;
    if (hasDefault) {
        return verifyYieldingBranch(getOperation(), regions.back());
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
    if (failed(verifyPassThrough(getOperation(), getColumns(), getResults()))) {
        return failure();
    }

    return verifyCutCount(getOperation(), getCount(), getBoundCount());
}

// db.skip passes its columns straight through, the same as db.limit.
LogicalResult Skip::verify() {
    if (failed(verifyPassThrough(getOperation(), getColumns(), getResults()))) {
        return failure();
    }

    return verifyCutCount(getOperation(), getCount(), getBoundCount());
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

bool FilterOp::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._operandOffset = 1, ._count = getColumnsToFilter().size()};
    return true;
}

bool Limit::getCarrySet(CarrySet& carrySet) {
    const MutableOperandRange carried = getCarrySetMutable();
    carrySet = CarrySet {._operandOffset = operandRunOffset(carried), ._count = carried.size()};
    return true;
}

MutableOperandRange Limit::getCarrySetMutable() {
    return getColumnsMutable();
}

bool Skip::getCarrySet(CarrySet& carrySet) {
    const MutableOperandRange carried = getCarrySetMutable();
    carrySet = CarrySet {._operandOffset = operandRunOffset(carried), ._count = carried.size()};
    return true;
}

MutableOperandRange Skip::getCarrySetMutable() {
    return getColumnsMutable();
}

bool Sort::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._count = getColumns().size()};
    return true;
}

bool RemoveDuplicates::getCarrySet(CarrySet& carrySet) {
    const MutableOperandRange carried = getCarrySetMutable();
    carrySet = CarrySet {._operandOffset = operandRunOffset(carried), ._count = carried.size()};
    return true;
}

MutableOperandRange RemoveDuplicates::getCarrySetMutable() {
    return getColumnsMutable();
}

void Union::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    collectBranchYields(value, getBranches(), carried);
}

void ScanMaterialized::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    Materialize materialize = getRows().getDefiningOp<Materialize>();
    if (materialize) {
        collectCarriedResult(value, 0, materialize.getColumns(), carried);
    }
}
