#pragma once

#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"

namespace db {

namespace nl = mlir::nl;

using NLUnaryFunctionEmitter = mlir::Value (*)(mlir::OpBuilder& builder,
                                               mlir::Location loc,
                                               nl::ChunkType resultType,
                                               mlir::Value input,
                                               mlir::Operation* source);

enum class ResultNullability {
    FollowsInput,
    AlwaysNullable,
    NeverNullable,
};

struct UnaryFunctionLowering {
    NLUnaryFunctionEmitter emit {nullptr};
    ResultNullability nullability {ResultNullability::FollowsInput};
};

const UnaryFunctionLowering* lookupUnaryFunctionLowering(mlir::Operation& operation);

using NLBinaryFunctionEmitter = mlir::Value (*)(mlir::OpBuilder& builder,
                                                mlir::Location loc,
                                                nl::ChunkType resultType,
                                                mlir::Value lhs,
                                                mlir::Value rhs);

using BinaryFunctionElement = mlir::Type (*)(mlir::OpBuilder& builder, mlir::Type lhsElement);

struct BinaryFunctionLowering {
    NLBinaryFunctionEmitter emit {nullptr};
    BinaryFunctionElement element {nullptr};
};

const BinaryFunctionLowering* lookupBinaryFunctionLowering(mlir::Operation& operation);

}
