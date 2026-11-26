#include "NLExecutor.h"

#include <algorithm>

#include <spdlog/fmt/bundled/format.h>

#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/UnaryPredicates.h"
#include "QueryTypeName.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "columns/BinaryPredicates.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLColumnKernels.h"

#include "LocalMemory.h"
#include "IRException.h"

using namespace db;

namespace {

void collectBooleanSurvivors(const Column* mask, ColumnVector<size_t>* indices) {
    const std::vector<CustomBool>& maskRaw = static_cast<const ColumnVector<CustomBool>*>(mask)->getRaw();
    std::vector<size_t>& survivingRaw = indices->getRaw();

    for (size_t row = 0; row < maskRaw.size(); row++) {
        if (maskRaw[row]) {
            survivingRaw.push_back(row);
        }
    }
}

void collectOptBooleanSurvivors(const Column* mask, ColumnVector<size_t>* indices) {
    const ColumnOptVector<CustomBool>* typedMask = static_cast<const ColumnOptVector<CustomBool>*>(mask);
    std::vector<size_t>& survivingRaw = indices->getRaw();

    const size_t rowCount = typedMask->size();
    for (size_t row = 0; row < rowCount; row++) {
        if (typedMask->isValid(row) && typedMask->getValue(row)) {
            survivingRaw.push_back(row);
        }
    }
}

// A null mask holds for no row, so a filter on one keeps nothing
void collectNoSurvivors(const Column*, ColumnVector<size_t>*) {
}

// The truth value a type-erased cell holds: its boolean, or null where it holds a null
std::optional<bool> cellTruth(CellView cell) {
    const CellTag tag = cell.getTag();

    switch (tag) {
        case CellTag::Bool:
            return static_cast<bool>(cell.getAs<types::Bool::Primitive>());
        break;

        case CellTag::Null:
            return std::nullopt;
        break;

        default:
            throw IRException(fmt::format("A condition must be a boolean, not '{}'", queryTypeName(tag)));
        break;
    }
}

void collectTaggedCellSurvivors(const Column* mask, ColumnVector<size_t>* indices) {
    const std::vector<CellView>& cells = static_cast<const ColumnVector<CellView>*>(mask)->getRaw();
    std::vector<size_t>& survivingRaw = indices->getRaw();

    for (size_t row = 0; row < cells.size(); row++) {
        if (cellTruth(cells[row]).value_or(false)) {
            survivingRaw.push_back(row);
        }
    }
}

void applyNotOnBooleans(Column* result, const Column* operand) {
    const std::vector<CustomBool>& operandRaw = static_cast<const ColumnVector<CustomBool>*>(operand)->getRaw();
    std::vector<CustomBool>& resultRaw = static_cast<ColumnVector<CustomBool>*>(result)->getRaw();

    resultRaw.resize(operandRaw.size());
    std::transform(operandRaw.begin(), operandRaw.end(), resultRaw.begin(), [](CustomBool value) {
        return CustomBool {!value};
    });
}

void applyNotOnOptBooleans(Column* result, const Column* operand) {
    UnaryPredicateExecutor<Not>::apply(static_cast<ColumnOptVector<CustomBool>*>(result),
                                       static_cast<const ColumnOptVector<CustomBool>*>(operand));
}

std::optional<CustomBool> negatedTruth(const auto& cell) {
    const std::optional<CustomBool> truth = truthValue(cell);
    if (!truth.has_value()) {
        return std::nullopt;
    }

    return CustomBool {!truth->_boolean};
}

void applyNotOnTaggedCells(Column* result, const Column* operand) {
    const std::vector<CellView>& operandRaw = static_cast<const ColumnVector<CellView>*>(operand)->getRaw();
    ColumnOptVector<CustomBool>* typedResult = static_cast<ColumnOptVector<CustomBool>*>(result);

    resetForRowWrites(typedResult, operandRaw.size());
    for (size_t row = 0; row < operandRaw.size(); row++) {
        typedResult->setOptional(row, negatedTruth(operandRaw[row]));
    }
}

void applyNotOnConstTaggedCell(Column* result, const Column* operand) {
    const CellView cell = static_cast<const ColumnConst<CellView>*>(operand)->getRaw();
    static_cast<ColumnConst<std::optional<CustomBool>>*>(result)->setOptional(negatedTruth(cell));
}

void applyNotOnConst(Column* result, const Column* operand) {
    const ColumnConst<CustomBool>* typedOperand = static_cast<const ColumnConst<CustomBool>*>(operand);
    ColumnConst<CustomBool>* typedResult = static_cast<ColumnConst<CustomBool>*>(result);
    UnaryPredicateExecutor<Not>::apply(typedResult, typedOperand);
}

void applyNotOnOptConst(Column* result, const Column* operand) {
    using OptBool = std::optional<CustomBool>;
    UnaryPredicateExecutor<Not>::apply(static_cast<ColumnConst<OptBool>*>(result),
                                       static_cast<const ColumnConst<OptBool>*>(operand));
}

// NOT of an unknown truth value is unknown; the ColumnConst<PropertyNull> result already
// reads as null, so nothing is computed.
void applyNotOnNullConst(Column*, const Column*) {
}

bool caseTestOptBoolean(const Column* condition, size_t row) {
    const ColumnOptVector<CustomBool>* typedCondition = static_cast<const ColumnOptVector<CustomBool>*>(condition);

    return typedCondition->isValid(row) && typedCondition->getValue(row);
}

// A null is not true, so a branch whose condition is the null literal never fires and its
// rows fall through to the next one
bool caseTestNever(const Column* condition, size_t row) {
    return false;
}

void applyIsNotTrueOnOptBooleans(Column* result, const Column* operand) {
    NullablePredicateExecutor::isNotTrue(static_cast<ColumnVector<CustomBool>*>(result),
                                         static_cast<const ColumnOptVector<CustomBool>*>(operand));
}

void applyIsNotTrueOnTaggedCells(Column* result, const Column* operand) {
    const std::vector<CellView>& operandRaw = static_cast<const ColumnVector<CellView>*>(operand)->getRaw();
    std::vector<CustomBool>& resultRaw = static_cast<ColumnVector<CustomBool>*>(result)->getRaw();

    resultRaw.resize(operandRaw.size());
    std::transform(operandRaw.begin(), operandRaw.end(), resultRaw.begin(), [](CellView cell) {
        return CustomBool {!cellTruth(cell).value_or(false)};
    });
}

void applyIsNotTrueOnNull(Column* result, const Column* operand) {
    static_cast<ColumnVector<CustomBool>*>(result)->getRaw().assign(operand->size(), CustomBool {true});
}

std::optional<bool> readBooleanTruth(const Column* mask, size_t row) {
    return static_cast<bool>(static_cast<const ColumnVector<CustomBool>*>(mask)->getRaw()[row]);
}

std::optional<bool> readOptBooleanTruth(const Column* mask, size_t row) {
    const ColumnOptVector<CustomBool>* typedMask = static_cast<const ColumnOptVector<CustomBool>*>(mask);
    if (!typedMask->isValid(row)) {
        return std::nullopt;
    }

    return static_cast<bool>(typedMask->getValue(row));
}

std::optional<bool> readNullTruth(const Column* mask, size_t row) {
    return std::nullopt;
}

std::optional<bool> readTaggedCellTruth(const Column* cells, size_t row) {
    return cellTruth(static_cast<const ColumnVector<CellView>*>(cells)->getRaw()[row]);
}

bool caseTestBoolean(const Column* condition, size_t row) {
    return readBooleanTruth(condition, row).value_or(false);
}

bool caseTestTaggedCell(const Column* condition, size_t row) {
    return readTaggedCellTruth(condition, row).value_or(false);
}

}

NLTruthReadFunction NLExecutor::selectTruthRead(NLTruthColumnKind kind) {
    switch (kind) {
        case NLTruthColumnKind::Booleans:
            return &readBooleanTruth;
        break;

        case NLTruthColumnKind::NullableBooleans:
            return &readOptBooleanTruth;
        break;

        case NLTruthColumnKind::TaggedCells:
            return &readTaggedCellTruth;
        break;

        case NLTruthColumnKind::UntypedNull:
            return &readNullTruth;
        break;
    }

    throw IRException("Unhandled truth column kind");
}

NLCaseTestFn NLExecutor::selectCaseTest(NLTruthColumnKind kind) {
    switch (kind) {
        case NLTruthColumnKind::Booleans:
            return &caseTestBoolean;
        break;

        case NLTruthColumnKind::NullableBooleans:
            return &caseTestOptBoolean;
        break;

        case NLTruthColumnKind::TaggedCells:
            return &caseTestTaggedCell;
        break;

        case NLTruthColumnKind::UntypedNull:
            return &caseTestNever;
        break;
    }

    throw IRException("Unhandled truth column kind");
}

NLUnaryFn NLExecutor::selectIsNotTrue(NLTruthColumnKind kind, LocalMemory* memory, Column*& result) {
    result = memory->alloc<ColumnVector<CustomBool>>();

    switch (kind) {
        case NLTruthColumnKind::Booleans:
            return &applyNotOnBooleans;
        break;

        case NLTruthColumnKind::NullableBooleans:
            return &applyIsNotTrueOnOptBooleans;
        break;

        case NLTruthColumnKind::TaggedCells:
            return &applyIsNotTrueOnTaggedCells;
        break;

        case NLTruthColumnKind::UntypedNull:
            return &applyIsNotTrueOnNull;
        break;
    }

    throw IRException("Unhandled truth column kind");
}

NLUnaryFn NLExecutor::selectNot(NLTruthColumnKind kind, LocalMemory* memory, Column*& result) {
    switch (kind) {
        case NLTruthColumnKind::Booleans:
            result = memory->alloc<ColumnVector<CustomBool>>();
            return &applyNotOnBooleans;
        break;

        case NLTruthColumnKind::NullableBooleans:
            result = memory->alloc<ColumnOptVector<CustomBool>>();
            return &applyNotOnOptBooleans;
        break;

        case NLTruthColumnKind::TaggedCells:
            result = memory->alloc<ColumnOptVector<CustomBool>>();
            return &applyNotOnTaggedCells;
        break;

        case NLTruthColumnKind::UntypedNull:
            throw IRException("nl.not reads the null literal only as a constant: rows of null are laid out as nullable booleans");
        break;
    }

    throw IRException("Unhandled truth column kind");
}

NLUnaryFn NLExecutor::selectConstantNot(NLTruthColumnKind kind, LocalMemory* memory, Column*& result) {
    switch (kind) {
        case NLTruthColumnKind::Booleans:
            result = memory->alloc<ColumnConst<CustomBool>>();
            return &applyNotOnConst;
        break;

        case NLTruthColumnKind::NullableBooleans:
            result = memory->alloc<ColumnConst<std::optional<CustomBool>>>();
            return &applyNotOnOptConst;
        break;

        case NLTruthColumnKind::TaggedCells:
            result = memory->alloc<ColumnConst<std::optional<CustomBool>>>();
            return &applyNotOnConstTaggedCell;
        break;

        case NLTruthColumnKind::UntypedNull:
            result = memory->alloc<ColumnConst<PropertyNull>>();
            return &applyNotOnNullConst;
        break;
    }

    throw IRException("Unhandled truth column kind");
}

NLMaskSurvivorFunction NLExecutor::selectMaskSurvivorFunction(NLTruthColumnKind kind) {
    switch (kind) {
        case NLTruthColumnKind::Booleans:
            return &collectBooleanSurvivors;
        break;

        case NLTruthColumnKind::NullableBooleans:
            return &collectOptBooleanSurvivors;
        break;

        case NLTruthColumnKind::TaggedCells:
            return &collectTaggedCellSurvivors;
        break;

        case NLTruthColumnKind::UntypedNull:
            return &collectNoSurvivors;
        break;
    }

    throw IRException("Unhandled truth column kind");
}
