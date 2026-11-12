#include "DBInterfaces.h"

#include "DBInterfaces.cpp.inc"

#include "DBOps.h"

#include "BioAssert.h"

using namespace mlir;

namespace {

MutableOperandRange unsegmentedRun(Operation* op, size_t offset, size_t count) {
    bioassert(!op->hasTrait<OpTrait::AttrSizedOperandSegments>(), "An op sizing its operand segments names the one holding each carry set");

    return MutableOperandRange(op, static_cast<unsigned>(offset), static_cast<unsigned>(count));
}

// The run of results the columns @param region yields come out as, row for row; false where
// the op holding it pads or merges them
bool matchYieldedRun(Region& region, size_t& resultOffset, size_t& count) {
    mlir::db::ColumnCarrierOpInterface carrier = dyn_cast<mlir::db::ColumnCarrierOpInterface>(region.getParentOp());
    if (!carrier || !carrier.getYieldedResultOffset(region, resultOffset)) {
        return false;
    }

    count = mlir::db::yieldedColumnsOf(region.front()).size();

    return true;
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

size_t mlir::db::forwardedArgumentCount(Region& region) {
    ColumnCarrierOpInterface carrier = dyn_cast_or_null<ColumnCarrierOpInterface>(region.getParentOp());
    if (!carrier) {
        return 0;
    }

    size_t count = 0;
    for (Region& earlier : region.getParentOp()->getRegions()) {
        if (&earlier == &region) {
            break;
        }

        if (carrier.forwardsColumns(earlier)) {
            count += carrier.getForwardedColumnsMutable(earlier).size();
        }
    }

    return count;
}

bool mlir::db::matchForwardedArgument(BlockArgument argument, Region*& source, size_t& forwardIndex) {
    Region& region = *argument.getParentRegion();
    ColumnCarrierOpInterface carrier = dyn_cast_or_null<ColumnCarrierOpInterface>(region.getParentOp());
    if (!carrier) {
        return false;
    }

    size_t argumentIndex = argument.getArgNumber();
    for (Region& earlier : region.getParentOp()->getRegions()) {
        if (&earlier == &region) {
            return false;
        }

        if (!carrier.forwardsColumns(earlier)) {
            continue;
        }

        const size_t forwardedCount = carrier.getForwardedColumnsMutable(earlier).size();
        if (argumentIndex < forwardedCount) {
            source = &earlier;
            forwardIndex = argumentIndex;
            return true;
        }

        argumentIndex -= forwardedCount;
    }

    return false;
}

bool mlir::db::matchRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    ColumnCarrierOpInterface carrier = dyn_cast_or_null<ColumnCarrierOpInterface>(region.getParentOp());

    return carrier && carrier.getRegionCarrySet(region, carrySet);
}

bool mlir::db::matchCarriedIndex(BlockArgument argument, size_t& carriedIndex) {
    BlockArgument held = argument;
    while (true) {
        Region& region = *held.getParentRegion();

        RegionCarrySet carrySet;
        if (!matchRegionCarrySet(region, carrySet)) {
            return false;
        }

        const size_t argumentIndex = held.getArgNumber();
        const bool isCarried = argumentIndex >= carrySet._argumentOffset
                            && argumentIndex < carrySet._argumentOffset + carrySet._count;
        if (isCarried) {
            carriedIndex = argumentIndex - carrySet._argumentOffset;
            return true;
        }

        Region* source = nullptr;
        size_t forwardIndex = 0;
        if (!matchForwardedArgument(held, source, forwardIndex)) {
            return false;
        }

        ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(region.getParentOp());
        const MutableOperandRange forwarded = carrier.getForwardedColumnsMutable(*source);
        BlockArgument sourceArgument = dyn_cast<BlockArgument>(forwarded[static_cast<unsigned>(forwardIndex)].get());
        if (!sourceArgument || sourceArgument.getParentRegion() != source) {
            return false;
        }

        held = sourceArgument;
    }
}

Value mlir::db::carriedColumnOf(BlockArgument argument) {
    size_t carriedIndex = 0;
    if (!matchCarriedIndex(argument, carriedIndex)) {
        return {};
    }

    Region& region = *argument.getParentRegion();

    RegionCarrySet carrySet;
    matchRegionCarrySet(region, carrySet);

    return region.getParentOp()->getOperand(carrySet._operandOffset + carriedIndex);
}

Value mlir::db::elementSourceOf(BlockArgument argument) {
    Region& region = *argument.getParentRegion();
    ColumnCarrierOpInterface carrier = dyn_cast_or_null<ColumnCarrierOpInterface>(region.getParentOp());
    if (!carrier) {
        return {};
    }

    size_t elementArgument = 0;
    const Value source = carrier.getRegionElementSource(region, elementArgument);
    const bool bindsTheElement = source && argument.getArgNumber() == elementArgument;

    return bindsTheElement ? source : Value();
}

MutableOperandRange mlir::db::yieldedColumnsOf(Block& body) {
    Operation* const terminator = body.getTerminator();

    if (Yield yield = dyn_cast<Yield>(terminator)) {
        return yield.getColumnsMutable();
    } else if (SubqueryYield yield = dyn_cast<SubqueryYield>(terminator)) {
        return yield.getColumnsMutable();
    }

    OptionalYield yield = cast<OptionalYield>(terminator);

    return yield.getColumnsMutable();
}

ResultRange mlir::db::yieldedResultsOf(Region& region) {
    const ResultRange results = region.getParentOp()->getResults();

    size_t resultOffset = 0;
    size_t yieldCount = 0;
    if (!matchYieldedRun(region, resultOffset, yieldCount)) {
        return results.take_front(0);
    }

    return results.slice(resultOffset, yieldCount);
}

bool mlir::db::matchYieldedResult(OpResult result, Region*& region, size_t& yieldIndex) {
    const size_t resultIndex = result.getResultNumber();
    for (Region& candidate : result.getOwner()->getRegions()) {
        size_t resultOffset = 0;
        size_t yieldCount = 0;
        if (!matchYieldedRun(candidate, resultOffset, yieldCount)) {
            continue;
        }

        const bool comesOutOfTheRegion = resultIndex >= resultOffset && resultIndex < resultOffset + yieldCount;
        if (comesOutOfTheRegion) {
            region = &candidate;
            yieldIndex = resultIndex - resultOffset;
            return true;
        }
    }

    return false;
}

Region* mlir::db::yieldingRegionOf(OpResult result) {
    Region* region = nullptr;
    size_t yieldIndex = 0;
    matchYieldedResult(result, region, yieldIndex);

    return region;
}

Value mlir::db::yieldedColumnOf(OpResult result) {
    Region* region = nullptr;
    size_t yieldIndex = 0;
    if (!matchYieldedResult(result, region, yieldIndex)) {
        return {};
    }

    const OperandRange yielded = yieldedColumnsOf(region->front());

    return yielded[yieldIndex];
}
