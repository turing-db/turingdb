#include "IRRaiseEffect.h"

#include "mlir/IR/Operation.h"

using namespace db;

llvm::StringRef RaiseResource::getName() const {
    return "Raise";
}

bool RaiseResource::isAddressable() const {
    return false;
}

void db::addRaiseEffect(mlir::Operation* operation, llvm::SmallVectorImpl<mlir::MemoryEffects::EffectInstance>& effects) {
    if (!mlir::isSpeculatable(operation)) {
        effects.emplace_back(mlir::MemoryEffects::Write::get(), RaiseResource::get());
    }
}

bool db::hasNoEffectButRaising(mlir::Operation* operation) {
    return operation->hasTrait<mlir::OpTrait::MayRaise>() || mlir::isMemoryEffectFree(operation);
}
