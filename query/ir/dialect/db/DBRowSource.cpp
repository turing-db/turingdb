#include "DBRowSource.h"

#include "DBInterfaces.h"

using namespace mlir;
using namespace mlir::db;

Value mlir::db::rowSourceOf(Value column) {
    if (const BlockArgument argument = dyn_cast<BlockArgument>(column)) {
        return carriedColumnOf(argument);
    }

    const OpResult result = cast<OpResult>(column);
    ColumnCarrierOpInterface carrier = dyn_cast<ColumnCarrierOpInterface>(result.getOwner());

    return carrier ? carrier.getRowSource(result) : Value();
}

Value mlir::db::stepToRowSource(Value column, ClimbsFrom climbsFrom) {
    return climbsFrom(column) ? rowSourceOf(column) : Value();
}

Value mlir::db::climbRowSource(Value column, ClimbsFrom climbsFrom) {
    Value climbed = column;
    while (const Value source = stepToRowSource(climbed, climbsFrom)) {
        climbed = source;
    }

    return climbed;
}

void mlir::db::visitDefinedValues(Operation* op, llvm::function_ref<void(Value)> visit) {
    op->walk([visit](Operation* nested) {
        for (const Value result : nested->getResults()) {
            visit(result);
        }

        for (Region& region : nested->getRegions()) {
            for (Block& block : region) {
                for (const BlockArgument argument : block.getArguments()) {
                    visit(argument);
                }
            }
        }
    });
}
