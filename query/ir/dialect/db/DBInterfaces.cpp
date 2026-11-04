#include "DBInterfaces.h"

#include "DBInterfaces.cpp.inc"

using namespace mlir;

Value mlir::db::carrySetRowSource(Operation* op, OpResult result) {
    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(op);

    CarrySet carrySet;
    const size_t resultNumber = result.getResultNumber();
    const bool carriesTheResult = carrier.getCarrySet(carrySet)
                               && resultNumber >= carrySet._resultOffset
                               && resultNumber < carrySet._resultOffset + carrySet._count;
    if (!carriesTheResult) {
        return {};
    }

    return op->getOperand(carrySet._operandOffset + resultNumber - carrySet._resultOffset);
}

void mlir::db::collectRowSource(Value value, llvm::SmallVectorImpl<Value>& carried) {
    const OpResult result = dyn_cast<OpResult>(value);
    if (!result) {
        return;
    }

    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(result.getOwner());
    if (const Value rowSource = carrier.getRowSource(result)) {
        carried.push_back(rowSource);
    }
}
