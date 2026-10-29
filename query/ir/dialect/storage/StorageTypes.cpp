#include "StorageTypes.h"

#include "StorageDialect.h"
#include "ValueTypeSetFormat.h"

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

#define GET_TYPEDEF_CLASSES
#include "StorageTypes.cpp.inc"

void Storage::registerTypes() {
    addTypes<
#define GET_TYPEDEF_LIST
#include "StorageTypes.cpp.inc"
    >();
}

mlir::LogicalResult DynamicType::verify(llvm::function_ref<mlir::InFlightDiagnostic()> emitError, uint32_t accepted) {
    const bool holdsValueTypesOnly = (accepted & ~::db::ValueTypeSet::any().getBits()) == 0;
    const ::db::ValueTypeSet set = ::db::ValueTypeSet::fromBits(accepted);

    if (!holdsValueTypesOnly) {
        return emitError() << "a dynamic type names value types only";
    } else if (accepted == 0 || set.isSingle()) {
        return emitError() << "a dynamic type names several value types; a value of one carries it as its type";
    }

    return mlir::success();
}

::db::ValueTypeSet DynamicType::getAcceptedSet() const {
    return ::db::ValueTypeSet::fromBits(getAccepted());
}



std::optional<::db::ValueType> mlir::storage::valueTypeOf(mlir::Type elementType) {
    if (mlir::isa<StringType>(elementType)) {
        return ::db::ValueType::String;
    } else if (mlir::isa<EmbeddingType>(elementType)) {
        return ::db::ValueType::Embedding;
    } else if (mlir::isa<ListType>(elementType)) {
        return ::db::ValueType::List;
    } else if (mlir::isa<DateTimeType>(elementType)) {
        return ::db::ValueType::DateTime;
    } else if (mlir::isa<DurationType>(elementType)) {
        return ::db::ValueType::Duration;
    } else if (mlir::isa<MapType>(elementType)) {
        return ::db::ValueType::Map;
    } else if (mlir::isa<mlir::Float64Type>(elementType)) {
        return ::db::ValueType::Double;
    } else if (const auto intType = mlir::dyn_cast<mlir::IntegerType>(elementType)) {
        if (intType.getWidth() == 1) {
            return ::db::ValueType::Bool;
        } else if (intType.isUnsigned()) {
            return ::db::ValueType::UInt64;
        }

        return ::db::ValueType::Int64;
    }

    return std::nullopt;
}
