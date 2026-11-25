#pragma once

#include <span>

#include "mlir/IR/Builders.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"

#include "StorageAttributes.h"
#include "StorageTypes.h"

#include "decl/PatternData.h"

namespace db {

template <SchemaNameKind Kind>
mlir::Type schemaNameParameterType(mlir::MLIRContext* context) {
    if constexpr (Kind == SchemaNameKind::Label) {
        return mlir::storage::LabelIDType::get(context);
    } else {
        return mlir::storage::EdgeTypeIDType::get(context);
    }
}

// The attribute a schema name stands as: the string, or the parameter typed as a label or
// an edge type
template <SchemaNameKind Kind>
mlir::Attribute schemaNameAttr(mlir::OpBuilder& builder, const SchemaNameRef<Kind>& ref) {
    const llvm::StringRef name(ref._name.data(), ref._name.size());
    if (ref._isParameter) {
        mlir::MLIRContext* const context = builder.getContext();
        return mlir::storage::ParameterAttr::get(context, name, schemaNameParameterType<Kind>(context));
    }

    return builder.getStringAttr(name);
}

template <SchemaNameKind Kind>
mlir::ArrayAttr schemaNamesAttr(mlir::OpBuilder& builder, std::span<const SchemaNameRef<Kind>> names) {
    llvm::SmallVector<mlir::Attribute> refs;
    for (const SchemaNameRef<Kind>& name : names) {
        refs.push_back(schemaNameAttr(builder, name));
    }

    return builder.getArrayAttr(refs);
}

}
