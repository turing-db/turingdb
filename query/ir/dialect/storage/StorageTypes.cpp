#include "StorageTypes.h"

#include "StorageDialect.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/DialectImplementation.h"
#include "llvm/ADT/TypeSwitch.h"

using namespace mlir::storage;

#include "StorageEnums.cpp.inc"

mlir::LogicalResult NullableType::verify(llvm::function_ref<mlir::InFlightDiagnostic()> emitError,
                                         mlir::Type valueType) {
    if (isa<NodeIDType, EdgeIDType>(valueType)) {
        return emitError() << "a node or edge column holds its own null as an invalid ID, so it is never nullable";
    }

    return mlir::success();
}

bool mlir::storage::reducesToADouble(AggregateKind kind, Type values) {
    const bool averagesNumbers = kind == AggregateKind::Avg && !isa<DurationType, PointType, AnyType>(values);

    return averagesNumbers
        || kind == AggregateKind::StDev
        || kind == AggregateKind::StDevP;
}

bool mlir::storage::reducesToADouble(GroupAggregateKind kind, Type values) {
    switch (kind) {
        case GroupAggregateKind::Avg:
        case GroupAggregateKind::AvgDistinct:
            return reducesToADouble(AggregateKind::Avg, values);
        break;

        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevP:
        case GroupAggregateKind::StDevDistinct:
        case GroupAggregateKind::StDevPDistinct:
            return true;
        break;

        case GroupAggregateKind::Count:
        case GroupAggregateKind::CountDistinct:
        case GroupAggregateKind::CountRows:
        case GroupAggregateKind::Sum:
        case GroupAggregateKind::SumDistinct:
        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max:
        case GroupAggregateKind::First:
            return false;
        break;
    }

    return false;
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

bool mlir::storage::namesASchemaID(Type type) {
    return isa<LabelIDType, EdgeTypeIDType, PropertyTypeIDType, ValueTypeType>(type);
}

#define GET_TYPEDEF_CLASSES
#include "StorageTypes.cpp.inc"

void Storage::registerTypes() {
    addTypes<
#define GET_TYPEDEF_LIST
#include "StorageTypes.cpp.inc"
    >();
}

