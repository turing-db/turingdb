#include "DBInterfaces.h"

#include "DBInterfaces.cpp.inc"

#include "BioAssert.h"

using namespace mlir;

namespace {

MutableOperandRange unsegmentedRun(Operation* op, size_t offset, size_t count) {
    bioassert(!op->hasTrait<OpTrait::AttrSizedOperandSegments>(), "An op sizing its operand segments names the one holding each carry set");

    return MutableOperandRange(op, static_cast<unsigned>(offset), static_cast<unsigned>(count));
}

}

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

MutableOperandRange mlir::db::unsegmentedCarrySet(Operation* op) {
    CarrySet carrySet;
    const bool carries = cast<ColumnCarrierOpInterface>(op).getCarrySet(carrySet);
    bioassert(carries, "The carry set an op is asked for is one it has");

    return unsegmentedRun(op, carrySet._operandOffset, carrySet._count);
}

MutableOperandRange mlir::db::unsegmentedRegionCarrySet(Operation* op) {
    RegionCarrySet carrySet;
    const bool carries = cast<ColumnCarrierOpInterface>(op).getRegionCarrySet(op->getRegion(0), carrySet);
    bioassert(carries, "The carry set into a region an op is asked for is one it has");

    return unsegmentedRun(op, carrySet._operandOffset, carrySet._count);
}

size_t mlir::db::operandRunOffset(const MutableOperandRange& run) {
    const MutableArrayRef<OpOperand> operands = run.getOwner()->getOpOperands();

    return static_cast<size_t>(run.begin() - operands.begin());
}
