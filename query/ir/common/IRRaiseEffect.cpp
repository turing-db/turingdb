#include "IRRaiseEffect.h"

#include <optional>

#include "mlir/IR/Operation.h"
#include "llvm/ADT/STLExtras.h"

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
    const std::optional<llvm::SmallVector<mlir::MemoryEffects::EffectInstance>> effects = mlir::getEffectsRecursively(operation);
    if (!effects) {
        return false;
    }

    return llvm::all_of(*effects, [](const mlir::MemoryEffects::EffectInstance& effect) {
        return mlir::isa<RaiseResource>(effect.getResource());
    });
}
