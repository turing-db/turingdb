#include "DBUnwindEqualityCross.h"

#include <stdint.h>

#include "mlir/IR/Operation.h"
#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/DenseSet.h"

#include "DBColumnElement.h"
#include "DBLookupPredicates.h"
#include "DBPassUtils.h"

using namespace mlir;
using namespace mlir::db;

namespace {

// The constant unwind a factor is nothing but, null for any other factor. Anything else
// in the region would be dropped along with it, so the unwind has to be all there is
// besides the yield of its one column.
UnwindConst matchUnwindFactor(Region& factor) {
    Block& block = factor.front();
    if (!llvm::hasNItems(block.getOperations(), 2)) {
        return nullptr;
    }

    Yield yield = dyn_cast<Yield>(block.getTerminator());
    if (!yield || yield.getColumns().size() != 1) {
        return nullptr;
    }

    return yield.getColumns().front().getDefiningOp<UnwindConst>();
}

// Whether = holds two elements equal. A double equals every double it compares equal to, so
// 0.0 and -0.0 are one value and a NaN equals none.
bool holdsEqualElements(ArrayAttr elements) {
    llvm::SmallDenseSet<Attribute, 8> seen;

    for (const Attribute element : elements) {
        const FloatAttr floating = dyn_cast<FloatAttr>(element);
        if (floating && floating.getValue().isNaN()) {
            continue;
        }

        const bool isZero = floating && floating.getValue().isZero();
        const Attribute value = isZero ? FloatAttr::get(floating.getType(), 0.0) : element;

        if (!seen.insert(value).second) {
            return true;
        }
    }

    return false;
}

// Whether one comparison per element stands in for what the unwound column is compared to.
// A heterogeneous list rides the type-erased column whose cells are compared by the tag
// each carries, which no set of typed comparisons reproduces, and two equal elements - 0.0
// and -0.0 among them - emit the rows they match once each, which a membership test does not.
bool elementsCompareDistinctly(UnwindConst unwind) {
    const ColumnType column = cast<ColumnType>(unwind.getResult().getType());
    if (isa<storage::AnyType>(column.getType())) {
        return false;
    }

    const ArrayAttr elements = unwind.getElements();
    const bool literalElements = llvm::all_of(elements, [](Attribute element) {
        return isa<IntegerAttr, FloatAttr, StringAttr>(element);
    });

    return literalElements && !holdsEqualElements(elements);
}

// Whether no two elements name one entity, and one at least names an entity: an element
// naming none matches no row, and two naming one entity would emit its rows once each
bool namesDistinctEntities(UnwindConst unwind) {
    llvm::SmallDenseSet<int64_t, 8> named;

    for (const Attribute element : unwind.getElements()) {
        int64_t entityID = 0;
        if (entityIDOfLiteral(element, entityID) && !named.insert(entityID).second) {
            return false;
        }
    }

    return !named.empty();
}

EntityIDOf findEntityIDOfElements(Value unwoundColumn) {
    for (Operation* const user : unwoundColumn.getUsers()) {
        if (EntityIDOf converted = dyn_cast<EntityIDOf>(user)) {
            return converted;
        }
    }

    return nullptr;
}

// On every row the filter keeps, the elements hold the value of the column they were
// compared to. A node or an edge equals an integer by the ID it carries, so a projection
// reads the elements back through id() of an entity column, never through the entity.
bool comparesAnEntity(Value comparedColumn) {
    return isa<storage::NodeIDType, storage::EdgeIDType>(::db::declaredColumnElement(comparedColumn));
}

bool unwindsIntegers(UnwindConst unwind) {
    const ColumnType column = cast<ColumnType>(unwind.getResult().getType());

    return column.getType().isSignlessInteger(64);
}

bool unwindsFloats(UnwindConst unwind) {
    const ColumnType column = cast<ColumnType>(unwind.getResult().getType());

    return isa<mlir::Float64Type>(column.getType());
}

// Whether a column holds integers. A column typed none is resolved only in lowering, and can
// turn out to hold floats or tagged cells.
bool holdsIntegers(Value column) {
    return ::db::declaredColumnElement(column).isInteger(64);
}

// Whether every value of the column equal to an element is that element. A number can equal
// another without being it - 32 = 32.0, 0 = -0.0 - which a string or a boolean cannot.
bool equalsOnlyTheElements(UnwindConst unwind, Value comparedColumn) {
    if (unwindsFloats(unwind)) {
        return false;
    } else if (unwindsIntegers(unwind)) {
        return holdsIntegers(comparedColumn);
    }

    return true;
}

// A count(*) reads how many rows a column has, never what it holds, so any column of the
// same rows stands in for the one it counts.
bool onlyCountsRows(Value column) {
    return llvm::all_of(column.getUsers(), [](Operation* user) {
        Count count = dyn_cast<Count>(user);
        return count && count.getRows();
    });
}

// An output writes out whatever column it is handed. Any other reader may spell the type of
// the column it reads, in its results, a block argument or a list it builds, so a column of
// another type cannot be put in its place.
bool onlyOutputsRead(Value column) {
    return llvm::all_of(column.getUsers(), [](Operation* user) {
        return isa<Output>(user);
    });
}

// The one equality the elements reach, as themselves or as the entities @param converted
// names, provided nothing else reads them but the filter that equality masks - so dropping
// the unwind leaves no other reader behind.
EqOp matchSoleEquality(Value unwoundColumn, EntityIDOf converted, FilterOp& filter) {
    const Value compared = converted ? converted.getResult() : unwoundColumn;

    EqOp equality;
    for (Operation* const user : compared.getUsers()) {
        const EqOp candidate = dyn_cast<EqOp>(user);
        if (!candidate) {
            continue;
        }

        if (equality) {
            return nullptr;
        }

        equality = candidate;
    }

    if (!equality || !equality.getResult().hasOneUse()) {
        return nullptr;
    }

    filter = dyn_cast<FilterOp>(*equality.getResult().getUsers().begin());
    if (!filter || filter.getMask() != equality.getResult()) {
        return nullptr;
    } else if (converted && !compared.hasOneUse()) {
        return nullptr;
    }

    Operation* const tester = converted ? converted.getOperation() : equality.getOperation();
    for (Operation* const user : unwoundColumn.getUsers()) {
        const bool testsTheColumn = user == tester;
        const bool carriesTheColumn = user == filter.getOperation();

        if (!testsTheColumn && !carriesTheColumn) {
            return nullptr;
        }
    }

    return equality;
}

}

bool mlir::db::matchUnwindEqualityCross(CrossProduct product, UnwindEqualityCross& match) {
    Region& leftFactor = product.getLeftFactor();
    Region& rightFactor = product.getRightFactor();

    const UnwindConst leftUnwind = matchUnwindFactor(leftFactor);
    const UnwindConst rightUnwind = matchUnwindFactor(rightFactor);

    // Two unwind factors have no relation between them to fold either into.
    if (static_cast<bool>(leftUnwind) == static_cast<bool>(rightUnwind)) {
        return false;
    }

    match._product = product;
    match._unwind = leftUnwind ? leftUnwind : rightUnwind;
    match._relationFactor = leftUnwind ? &rightFactor : &leftFactor;
    match._unwindOnTheLeft = static_cast<bool>(leftUnwind);

    // The results are the left factor's yielded columns followed by the right factor's, so
    // an unwind on the left contributes the first and one on the right the last.
    const size_t unwoundIndex = match._unwindOnTheLeft ? 0 : product.getResults().size() - 1;
    match._unwoundColumn = product.getResult(unwoundIndex);
    match._converted = findEntityIDOfElements(match._unwoundColumn);

    EntityIDOf converted = match._converted;
    const bool distinct = converted ? namesDistinctEntities(match._unwind) : elementsCompareDistinctly(match._unwind);
    if (!distinct) {
        return false;
    }

    match._equality = matchSoleEquality(match._unwoundColumn, converted, match._filter);
    if (!match._equality) {
        return false;
    }

    const Value compared = converted ? converted.getResult() : match._unwoundColumn;
    const Value lhs = match._equality.getLhs();
    match._comparedColumn = lhs == compared ? match._equality.getRhs() : lhs;

    // The compared column has to be one the rows still carry once the product is gone:
    // a column the relation yields, or a value computed from one. A column defined inside
    // a factor is dropped with it.
    Operation* const comparedDef = match._comparedColumn.getDefiningOp();
    const bool survivesTheProduct = comparedDef
                                    && comparedDef->getParentRegion() == product->getParentRegion();

    if (!survivesTheProduct) {
        return false;
    }

    // The disjunction is tested on every row of the relation, which only pays where it is a
    // lookup: node or edge IDs, which the scans by ID look up directly, or one element, a
    // single equality. A longer list of keys is a cut fuse_hash_join indexes.
    const bool comparesEntityIDs = isEntityColumn(match._comparedColumn);
    const bool unwindsOneElement = match._unwind.getElements().size() == 1;
    if (!comparesEntityIDs && (converted || !unwindsOneElement)) {
        return false;
    }

    // Codegen carries every column in flight, so the filter holding the elements does not
    // mean anything reads them: an unused result is dropped rather than stood in for, and
    // only a column something does read has to be one the compared column can replace.
    const Operation::operand_range carried = match._filter.getColumnsToFilter();
    const ResultRange filtered = match._filter.getFilteredColumns();

    const bool entityCompared = comparesAnEntity(match._comparedColumn);
    const bool integerElements = unwindsIntegers(match._unwind);
    const bool standsForTheElements = equalsOnlyTheElements(match._unwind, match._comparedColumn);

    // One element is what every row the filter keeps was compared equal to, whatever the
    // compared column holds, so it is read back as itself where no column stands for it.
    // Several integers compared to a node are read back as its ID, typed as the elements.
    size_t survivingColumns = 0;
    for (size_t index = 0; index < carried.size(); index++) {
        const bool holdsTheElements = carried[index] == match._unwoundColumn;

        if (holdsTheElements) {
            const Value elements = filtered[index];
            const bool readsTheValues = !onlyCountsRows(elements);
            const bool sameType = elements.getType() == match._comparedColumn.getType();
            const bool acceptsTheComparedType = sameType || onlyOutputsRead(elements);
            const bool readsTheComparedColumn = !entityCompared && standsForTheElements && acceptsTheComparedType;
            const bool readsTheElement = readsTheValues
                                      && !readsTheComparedColumn
                                      && unwindsOneElement
                                      && (converted || !entityCompared);
            const bool readsTheIDs = readsTheValues && entityCompared && !readsTheElement;

            if (elements.use_empty()) {
                continue;
            } else if (readsTheIDs && !integerElements) {
                return false;
            } else if (readsTheValues && !readsTheIDs && !readsTheComparedColumn && !readsTheElement) {
                return false;
            }

            match._readsTheIDs = match._readsTheIDs || readsTheIDs;
            match._readsTheElement = match._readsTheElement || readsTheElement;
        }

        survivingColumns++;
    }

    // A filter of nothing cuts nothing, so the elements were all it carried and the rows
    // it kept are read from the relation directly.
    if (survivingColumns == 0) {
        return false;
    }

    return true;
}
