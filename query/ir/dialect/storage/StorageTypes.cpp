#include "StorageTypes.h"

#include "StorageDialect.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/DialectImplementation.h"
#include "llvm/ADT/TypeSwitch.h"

using namespace mlir::storage;

#include "StorageEnums.cpp.inc"

bool mlir::storage::reducesToADouble(AggregateKind kind) {
    return kind == AggregateKind::Avg
        || kind == AggregateKind::StDev
        || kind == AggregateKind::StDevP;
}

mlir::Type mlir::storage::joinedElementType(Type lhs, Type rhs) {
    MLIRContext* const context = lhs.getContext();

    size_t listDepth = 0;
    Type joined;

    while (!joined) {
        const ListType lhsList = dyn_cast<ListType>(lhs);
        const ListType rhsList = dyn_cast<ListType>(rhs);

        if (lhs == rhs || isa<NoneType>(rhs)) {
            joined = lhs;
        } else if (isa<NoneType>(lhs)) {
            joined = rhs;
        } else if (lhsList && rhsList) {
            lhs = lhsList.getElementType();
            rhs = rhsList.getElementType();
            listDepth++;
        } else {
            joined = AnyType::get(context);
        }
    }

    for (size_t level = 0; level < listDepth; level++) {
        joined = ListType::get(context, joined);
    }

    return joined;
}

#define GET_TYPEDEF_CLASSES
#include "StorageTypes.cpp.inc"

void Storage::registerTypes() {
    addTypes<
#define GET_TYPEDEF_LIST
#include "StorageTypes.cpp.inc"
    >();
}

