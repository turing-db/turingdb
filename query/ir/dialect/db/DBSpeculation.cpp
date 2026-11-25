#include "DBSpeculation.h"

#include "columns/CharacterArgument.h"

#include "DBColumnChecks.h"
#include "IRLiteralList.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

namespace {

bool isCharacterLiteral(Value argument) {
    int64_t value = 0;

    return matchIntegerLiteral(argument, value) && value >= 0 && value <= ::db::MAX_CHARACTER_ARGUMENT;
}

}

Speculation::Speculatability mlir::db::speculatableWhen(bool cannotRaise) {
    return cannotRaise ? Speculation::Speculatable : Speculation::NotSpeculatable;
}

Speculation::Speculatability mlir::db::speculatableOverKnownTypes(ValueRange operands) {
    return speculatableWhen(llvm::none_of(operands, mayHoldAnyType));
}

bool mlir::db::matchIntegerLiteral(Value column, int64_t& value) {
    ConstantOp constant = column.getDefiningOp<ConstantOp>();
    const IntegerAttr integer = constant ? dyn_cast<IntegerAttr>(constant.getValue()) : IntegerAttr();
    if (!integer) {
        return false;
    }

    value = integer.getValue().getSExtValue();
    return true;
}

Speculation::Speculatability mlir::db::speculatableOverCharacters(Operation* op) {
    const bool readsAString = isa<storage::StringType>(columnElement(op->getOperand(0)));
    const bool countsLiterals = llvm::all_of(op->getOperands().drop_front(), isCharacterLiteral);

    return speculatableWhen(readsAString && countsLiterals);
}
