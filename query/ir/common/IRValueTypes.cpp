#include "IRValueTypes.h"

#include "mlir/IR/Builders.h"

#include "StorageTypes.h"

#include "IRException.h"

using namespace db;

mlir::Type db::valueTypeToElementType(mlir::OpBuilder& builder, ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return builder.getIntegerType(64);
        break;

        case ValueType::UInt64:
            return builder.getIntegerType(64, /*isSigned=*/false);
        break;

        case ValueType::Double:
            return builder.getF64Type();
        break;

        case ValueType::Bool:
            return builder.getI1Type();
        break;

        case ValueType::String:
            return mlir::storage::StringType::get(builder.getContext());
        break;

        case ValueType::Embedding:
            return mlir::storage::EmbeddingType::get(builder.getContext());
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("Invalid property value type");
        break;
    }

    throw IRException("Unhandled property value type");
}
