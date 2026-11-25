#include "NLOps.h"

#include <optional>

#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallVector.h"

#include "IRConstantColumn.h"
#include "IRLiteralList.h"
#include "NullLiteralType.h"
#include "NLChunkTypes.h"

using namespace mlir;
using namespace mlir::nl;

namespace storage = mlir::storage;

namespace {

template <typename CaseOp>
LogicalResult verifyCaseWrites(CaseOp caseOp) {
    const auto valueTypes = caseOp.getValues().getTypes();
    llvm::SmallVector<Type, 4> writtenTypes(valueTypes.begin(), valueTypes.end());
    if (const Value defaultValue = caseOp.getDefaultValue()) {
        writtenTypes.push_back(defaultValue.getType());
    }

    return verifyCellWrites(caseOp.getOperation(), caseOp.getResult().getType(), writtenTypes);
}

}

LogicalResult Constant::inferReturnTypes(MLIRContext* context,
                                         std::optional<Location> location,
                                         Constant::Adaptor adaptor,
                                         SmallVectorImpl<Type>& inferredReturnTypes) {
    // The db-dialect sibling's rule: a list, an embedding and a map are typed by literalType
    if (isa<ArrayAttr, DenseF32ArrayAttr, DictionaryAttr>(adaptor.getValue())) {
        inferredReturnTypes.push_back(ChunkType::get(context, ::db::literalType(adaptor.getValue())));
        return success();
    }

    // Inference runs during parsing, ahead of the operand constraint, so an attribute that
    // is neither typed nor an array is rejected here rather than cast blindly.
    const auto value = dyn_cast<TypedAttr>(adaptor.getValue());
    if (!value) {
        return emitOptionalError(location,
                                 "nl.constant carries a typed value, a dense f32 array or "
                                 "an array of literals, not ", adaptor.getValue());
    }

    inferredReturnTypes.push_back(ChunkType::get(context, value.getType()));
    return success();
}

LogicalResult Eq::verify() {
    const Type lhsElement = cast<ChunkType>(getLhs().getType()).getElementType();
    const Type rhsElement = cast<ChunkType>(getRhs().getType()).getElementType();

    return verifyComparesNoNullLiteral(getOperation(), lhsElement, rhsElement);
}

LogicalResult Neq::verify() {
    const Type lhsElement = cast<ChunkType>(getLhs().getType()).getElementType();
    const Type rhsElement = cast<ChunkType>(getRhs().getType()).getElementType();

    return verifyComparesNoNullLiteral(getOperation(), lhsElement, rhsElement);
}

LogicalResult Case::verify() {
    return verifyCaseWrites(*this);
}

LogicalResult CaseMerge::verify() {
    return verifyCaseWrites(*this);
}

LogicalResult FillNulls::verify() {
    const Type resultType = getResult().getType();
    const Type nullRowsElement = cast<ChunkType>(getNullRows().getType()).getElementType();

    const bool valueHasTheResultType = getValue().getType() == resultType;
    const bool fillHasTheResultType = getFill().getType() == resultType;

    if (!valueHasTheResultType || !fillHasTheResultType) {
        return emitOpError("fills a chunk out of a chunk of its own type, into a chunk of that type");
    } else if (!isa<storage::BoolType>(nullRowsElement)) {
        return emitOpError("marks the null rows it fills with a mask");
    }

    return success();
}

// An nl.to_any answers tagged cells, or out of lists the lists their elements join into,
// nullable when the lists it reads are
LogicalResult ToAny::verify() {
    const Type operandElement = cast<ChunkType>(getOperand().getType()).getElementType();
    const Type resultElement = cast<ChunkType>(getResult().getType()).getElementType();

    if (isa<storage::AnyType>(resultElement)) {
        return verifyReadAsCells(getOperation(), getOperand().getType());
    }

    const auto nullableResult = dyn_cast<storage::NullableType>(resultElement);
    const auto nullableOperand = dyn_cast<storage::NullableType>(operandElement);
    if (static_cast<bool>(nullableResult) != static_cast<bool>(nullableOperand)) {
        return emitOpError("keeps a list column nullable exactly when its operand is");
    }

    const Type resultList = nullableResult ? nullableResult.getValueType() : resultElement;
    const Type operandList = nullableOperand ? nullableOperand.getValueType() : operandElement;

    const bool readsAList = isa<storage::ListType>(resultList) && isa<storage::ListType>(operandList);
    if (!readsAList || storage::joinedElementType(operandList, resultList) != resultList) {
        return emitOpError("answers tagged cells, or for a list operand a list its elements join into");
    }

    return success();
}

LogicalResult RowConstant::verify() {
    llvm::SmallVector<Value> worklist {getRow()};
    llvm::DenseSet<Value> visited;

    while (!worklist.empty()) {
        const Value value = worklist.pop_back_val();
        if (!visited.insert(value).second || ::db::isFunctionArgument(value)) {
            continue;
        }

        Operation* const definingOp = value.getDefiningOp();
        if (definingOp) {
            worklist.append(definingOp->operand_begin(), definingOp->operand_end());
            continue;
        }

        For loop = dyn_cast<For>(cast<BlockArgument>(value).getOwner()->getParentOp());
        const bool bindsOneRow = loop && loop.getIterator().getDefiningOp<EachRow>();
        if (!bindsOneRow) {
            return emitOpError("operand must be computed from constants or from the one row of an nl.each_row step, "
                               "not from the rows of a step");
        }
    }

    return success();
}
