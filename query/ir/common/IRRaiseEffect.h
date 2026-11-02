#pragma once

#include "mlir/IR/OpDefinition.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "llvm/ADT/SmallVector.h"

namespace db {

// What a raise writes: the outcome of the query, which no memory aliases
struct RaiseResource : public mlir::SideEffects::Resource::Base<RaiseResource> {
    llvm::StringRef getName() const final;
    bool isAddressable() const final;
};

// A raise is a write to the RaiseResource where @param operation may raise, so MLIR's dead-op
// test keeps it though nothing reads it, as LLVM keeps an instruction that may throw
void addRaiseEffect(mlir::Operation* operation, llvm::SmallVectorImpl<mlir::MemoryEffects::EffectInstance>& effects);

// Whether @param operation has no effect, or none but a raise
bool hasNoEffectButRaising(mlir::Operation* operation);

}

namespace mlir::OpTrait {

// An op whose one effect is the raise it may make on some values, which is none where it is
// speculatable
template <typename ConcreteType>
class MayRaise : public TraitBase<ConcreteType, MayRaise> {
public:
    void getEffects(llvm::SmallVectorImpl<MemoryEffects::EffectInstance>& effects) {
        ::db::addRaiseEffect(this->getOperation(), effects);
    }
};

}
