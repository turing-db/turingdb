#include "StorageAttributes.h"

#include <optional>

#include "StorageDialect.h"
#include "StorageTypes.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/DialectImplementation.h"
#include "llvm/ADT/TypeSwitch.h"

#include "metadata/ValueTypeSet.h"

using namespace mlir::storage;

#define GET_ATTRDEF_CLASSES
#include "StorageAttributes.cpp.inc"

mlir::LogicalResult ParameterAttr::verify(llvm::function_ref<mlir::InFlightDiagnostic()> emitError,
                                          llvm::StringRef name,
                                          mlir::Type type) {
    if (name.empty()) {
        return emitError() << "a parameter needs a name";
    } else if (mlir::isa<mlir::NoneType>(type)) {
        return emitError() << "a parameter needs a type";
    }

    return mlir::success();
}

::db::ValueTypeSet ParameterAttr::getAcceptedSet() const {
    const mlir::Type type = getType();
    if (const DynamicType dynamic = mlir::dyn_cast<DynamicType>(type)) {
        return dynamic.getAcceptedSet();
    }

    const std::optional<::db::ValueType> columnType = valueTypeOf(type);
    return columnType ? ::db::ValueTypeSet::of(*columnType) : ::db::ValueTypeSet();
}

void Storage::registerAttributes() {
    addAttributes<
#define GET_ATTRDEF_LIST
#include "StorageAttributes.cpp.inc"
    >();
}
