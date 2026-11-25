#include "DBOps.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "DBColumnChecks.h"
#include "DBRegionColumns.h"
#include "DBSpeculation.h"
#include "IRConstantColumn.h"
#include "IRLiteralList.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

namespace {

// A literal list typed as homogeneous - db.unwind_const's typed column, db.const_list's
// typed list - must carry an element naming a type, and its elements' types must join to
// one element type; a null rides a unit attr and joins whatever the others carry.
// The elements are checked against each other and not against the spelled element type: a
// hand-written "s" parses as an untyped StringAttr, so comparing it to a !storage.string
// would reject valid IR. A homogeneous result paired with literals of another type is
// therefore not an op-level error; the runtime fill catches that on the element's type tag.
LogicalResult verifyHomogeneousElements(Operation* op, ArrayAttr elements) {
    const mlir::Type sharedType = ::db::sharedLiteralElementType(elements);

    if (isa<NoneType>(sharedType)) {
        return op->emitOpError("a homogeneous literal list must carry an element naming its type; "
                               "an empty list or a list of nulls alone is the !storage.any form");
    } else if (isa<storage::AnyType>(sharedType)) {
        return op->emitOpError("a homogeneous literal list requires every element to share one type");
    }

    return success();
}

// A value a db.vector_search reads when it starts: one value for every row the search is
// paired with, computed from parameters and literals or read off the row a body runs for.
// A cell, or an element lowering resolves, is checked when the search starts.
LogicalResult verifySearchedValue(Operation* op, llvm::StringRef name, Value bound, Type element) {
    if (!bound) {
        return success();
    }

    const Type boundElement = columnElement(bound);
    if (boundElement != element && !isa<NoneType, storage::AnyType>(boundElement)) {
        return op->emitOpError("requires a bound ") << name << " of type " << element << " or !storage.any, got " << boundElement;
    } else if (!::db::readsNoRow(bound) && !::db::yieldsConstantColumn(bound)) {
        return op->emitOpError("requires a bound ") << name << " holding one value for every row";
    }

    return success();
}

// A carried column comes back replicated, never retyped, so the carry set and the
// results past the element column must match one for one.
LogicalResult verifyCarriedResults(Operation* op, OperandRange carriedColumns, ResultRange carriedResults) {
    if (carriedColumns.size() != carriedResults.size()) {
        return op->emitOpError("expects one carried result per carried column, but has ")
               << carriedResults.size() << " for " << carriedColumns.size();
    }

    for (size_t carriedIndex = 0; carriedIndex < carriedColumns.size(); carriedIndex++) {
        if (carriedColumns[carriedIndex].getType() != carriedResults[carriedIndex].getType()) {
            return op->emitOpError("carried result ") << carriedIndex
                                                      << " must have the same type as carried column "
                                                      << carriedIndex;
        }
    }

    return success();
}

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

LogicalResult Unwind::verify() {
    return verifyCarriedResults(getOperation(), getColumnsToFilter(), getCarried());
}

// The source block takes one argument per carried column, of its type, and ends naming
// the source.
LogicalResult UnwindComputed::verify() {
    Block& sourceBlock = getSource().front();

    if (sourceBlock.empty() || !isa<UnwindYield>(sourceBlock.back())) {
        return emitOpError("source region must end with a db.unwind_yield");
    }

    const OperandRange carried = getColumnsToFilter();
    if (sourceBlock.getNumArguments() != carried.size()) {
        return emitOpError("source region takes one argument per carried column, ")
               << "expected " << carried.size() << " but has " << sourceBlock.getNumArguments();
    }

    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        const Type argumentType = sourceBlock.getArgument(static_cast<unsigned>(carriedIndex)).getType();

        if (argumentType != carried[carriedIndex].getType()) {
            return emitOpError("source argument ") << carriedIndex
                                                   << " must have the type of carried column "
                                                   << carriedIndex;
        }
    }

    return verifyCarriedResults(getOperation(), carried, getCarried());
}

LogicalResult UnwindRange::verify() {
    const ColumnType elementColumn = dyn_cast<ColumnType>(getElement().getType());
    const Type element = elementColumn ? elementColumn.getType() : Type {};
    const storage::NullableType nullable = dyn_cast_or_null<storage::NullableType>(element);

    const bool nullableInteger = nullable && isa<IntegerType>(nullable.getValueType());
    const bool resolvedInLowering = isa_and_nonnull<NoneType>(element);
    if (!nullableInteger && !resolvedInLowering) {
        return emitOpError("element must be a column of nullable integers, or of none for lowering to resolve");
    }

    return verifyCarriedResults(getOperation(), getColumnsToFilter(), getCarried());
}

// The unwound column's element type is the homogeneity verdict: a type-erased
// A !storage.any column accepts any elements, a typed one requires them to share that one
// type - the shared check the const_list verifier runs too.
LogicalResult UnwindConst::verify() {
    const ColumnType resultColumn = llvm::dyn_cast<ColumnType>(getResult().getType());
    if (!resultColumn) {
        return emitOpError("result must be a column");
    }

    const mlir::Type elementType = resultColumn.getType();
    if (llvm::isa<storage::AnyType>(elementType)) {
        return success();
    }

    return verifyHomogeneousElements(getOperation(), getElements());
}

// One column per field, and a field is named either by position or by header - a
// header only where a header line was read, since nothing else resolves the name.
LogicalResult LoadCSV::verify() {
    const Value boundPath = getBoundPath();
    if (getPath().has_value() == static_cast<bool>(boundPath)) {
        return emitOpError("requires either a path or a bound path");
    }

    const Type boundType = boundPath ? cast<ColumnType>(boundPath.getType()).getType() : Type();
    if (boundType && !isa<storage::StringType>(boundType)) {
        return emitOpError("requires a bound path of type !storage.string, got ") << boundType;
    }

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
    const Value boundIndexName = getBoundIndexName();
    if (getIndexName().has_value() == static_cast<bool>(boundIndexName)) {
        return emitOpError("requires either an index name or a bound one");
    }

    const std::optional<uint64_t> neighbourCount = getK();
    const Value boundCount = getBoundK();
    if (neighbourCount.has_value() == static_cast<bool>(boundCount)) {
        return emitOpError("requires either a neighbour count or a bound one");
    } else if (neighbourCount == 0) {
        return emitOpError("must report at least one neighbour");
    }

    const std::optional<llvm::ArrayRef<float>> queryVector = getQueryVector();
    const Value boundQueryVector = getBoundQueryVector();
    if (queryVector.has_value() == static_cast<bool>(boundQueryVector)) {
        return emitOpError("requires either a query vector or a bound one");
    } else if (queryVector && queryVector->empty()) {
        return emitOpError("must search for a vector of at least one dimension");
    }

    Operation* const op = getOperation();
    MLIRContext* const context = getContext();

    const bool boundValuesVerify = succeeded(verifySearchedValue(op, "index name", boundIndexName, storage::StringType::get(context)))
                                && succeeded(verifySearchedValue(op, "neighbour count", boundCount, IntegerType::get(context, 64)))
                                && succeeded(verifySearchedValue(op, "query vector", boundQueryVector, storage::EmbeddingType::get(context)));

    return success(boundValuesVerify);
}

bool Unwind::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._operandOffset = 1, ._resultOffset = 1, ._count = getColumnsToFilter().size()};
    return true;
}

bool UnwindRange::getCarrySet(CarrySet& carrySet) {
    const MutableOperandRange carried = getCarrySetMutable();
    carrySet = CarrySet {._operandOffset = operandRunOffset(carried), ._resultOffset = 1, ._count = carried.size()};
    return true;
}

MutableOperandRange UnwindRange::getCarrySetMutable() {
    return getColumnsToFilterMutable();
}

bool UnwindComputed::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._resultOffset = 1, ._count = getColumnsToFilter().size()};
    return true;
}

void UnwindComputed::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    collectRowSource(value, carried);
    collectCarriedArgument(value, getColumnsToFilter(), carried);
}

bool CallProcedure::getCarrySet(CarrySet& carrySet) {
    const MutableOperandRange carried = getCarrySetMutable();
    carrySet = CarrySet {._operandOffset = operandRunOffset(carried),
                         ._resultOffset = getYields().size(),
                         ._count = carried.size()};
    return true;
}

MutableOperandRange CallProcedure::getCarrySetMutable() {
    return getCarriedColumnsMutable();
}

// The window raises as the slice it was folded from does, on a type-erased bound holding no
// integer
Speculation::Speculatability UnwindRange::getSpeculatability() {
    int64_t by = 0;
    const bool countsByALiteralStep = matchIntegerLiteral(getStep(), by) && by != 0;
    if (!countsByALiteralStep) {
        return Speculation::NotSpeculatable;
    }

    llvm::SmallVector<Value, 4> bounds {getStart(), getEnd()};
    if (const Value from = getFrom()) {
        bounds.push_back(from);
    }

    if (const Value to = getTo()) {
        bounds.push_back(to);
    }

    return speculatableWhen(llvm::all_of(bounds, holdsOnlyIntegers));
}
