#include "DBOps.h"

#include "llvm/ADT/STLExtras.h"

using namespace mlir;
using namespace mlir::db;

namespace {

constexpr size_t HOP_FIXED_RESULT_COUNT = 4;

bool fillHopCarrySet(Operation* hop, CarrySet& carrySet) {
    carrySet = CarrySet {._operandOffset = 1, ._resultOffset = HOP_FIXED_RESULT_COUNT, ._count = hop->getNumOperands() - 1};
    return true;
}

// A hop hands the node it walks from back as one of its fixed results, @param inputResult
Value hopRowSource(Operation* hop, size_t inputResult, OpResult result) {
    if (result.getResultNumber() == inputResult) {
        return hop->getOperand(0);
    }

    return carrySetRowSource(hop, result);
}

}

// Ensures each variable has a numeric name
void GetOutEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetInEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetOutEdgesByType::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetInEdgesByType::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetOutEdgesByLabel::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetInEdgesByLabel::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetOutEdgesByTypeAndLabel::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetInEdgesByTypeAndLabel::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

LogicalResult GetOutEdgesByLabel::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult GetInEdgesByLabel::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult GetOutEdgesByTypeAndLabel::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult GetInEdgesByTypeAndLabel::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

bool GetOutEdges::getCarrySet(CarrySet& carrySet) {
    return fillHopCarrySet(getOperation(), carrySet);
}

Value GetOutEdges::getRowSource(OpResult result) {
    return hopRowSource(getOperation(), 0, result);
}

bool GetInEdges::getCarrySet(CarrySet& carrySet) {
    return fillHopCarrySet(getOperation(), carrySet);
}

Value GetInEdges::getRowSource(OpResult result) {
    return hopRowSource(getOperation(), 3, result);
}

bool GetEdges::getCarrySet(CarrySet& carrySet) {
    return fillHopCarrySet(getOperation(), carrySet);
}

Value GetEdges::getRowSource(OpResult result) {
    return hopRowSource(getOperation(), 0, result);
}

bool GetOutEdgesByType::getCarrySet(CarrySet& carrySet) {
    return fillHopCarrySet(getOperation(), carrySet);
}

Value GetOutEdgesByType::getRowSource(OpResult result) {
    return hopRowSource(getOperation(), 0, result);
}

bool GetInEdgesByType::getCarrySet(CarrySet& carrySet) {
    return fillHopCarrySet(getOperation(), carrySet);
}

Value GetInEdgesByType::getRowSource(OpResult result) {
    return hopRowSource(getOperation(), 3, result);
}

bool GetOutEdgesByLabel::getCarrySet(CarrySet& carrySet) {
    return fillHopCarrySet(getOperation(), carrySet);
}

Value GetOutEdgesByLabel::getRowSource(OpResult result) {
    return hopRowSource(getOperation(), 0, result);
}

bool GetInEdgesByLabel::getCarrySet(CarrySet& carrySet) {
    return fillHopCarrySet(getOperation(), carrySet);
}

Value GetInEdgesByLabel::getRowSource(OpResult result) {
    return hopRowSource(getOperation(), 3, result);
}

bool GetOutEdgesByTypeAndLabel::getCarrySet(CarrySet& carrySet) {
    return fillHopCarrySet(getOperation(), carrySet);
}

Value GetOutEdgesByTypeAndLabel::getRowSource(OpResult result) {
    return hopRowSource(getOperation(), 0, result);
}

bool GetInEdgesByTypeAndLabel::getCarrySet(CarrySet& carrySet) {
    return fillHopCarrySet(getOperation(), carrySet);
}

Value GetInEdgesByTypeAndLabel::getRowSource(OpResult result) {
    return hopRowSource(getOperation(), 3, result);
}
