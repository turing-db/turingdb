#include "DBOps.h"

#include <optional>

#include "mlir/IR/PatternMatch.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "DBColumnElement.h"
#include "DBColumnChecks.h"
#include "DBRegionColumns.h"
#include "DBRowCosts.h"
#include "DBSpeculation.h"
#include "IRConstantColumn.h"
#include "IRLiteralList.h"
#include "NullLiteralType.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

namespace {

// A selection whose result names an element holds the one its values select into. A result
// of none leaves it to lowering, and so does a value of none, which a pass can put in place
// of a typed one after codegen typed the result
LogicalResult verifySelectedResult(Operation* op, Value result, ValueRange values) {
    const Type resultElement = cast<ColumnType>(result.getType()).getType();
    const bool selectsAValueLoweringTypes = llvm::any_of(values, [](Value value) {
        return isa<NoneType>(cast<ColumnType>(value.getType()).getType());
    });

    if (isa<NoneType>(resultElement) || selectsAValueLoweringTypes) {
        return success();
    }

    const Type selectedElement = ::db::selectedColumnElement(op->getContext(), values);
    if (resultElement != selectedElement) {
        return op->emitOpError("result holds ") << resultElement << " but its values select into " << selectedElement;
    }

    return success();
}

bool isNumberColumn(Value column) {
    return isa<IntegerType, FloatType>(columnElement(column));
}

// Over numbers alone: a duration or an instant overflows, and a column of an unresolved
// type may hold either
Speculation::Speculatability speculatableOverNumbers(Operation* op) {
    return speculatableWhen(llvm::all_of(op->getOperands(), isNumberColumn));
}

// A literal divisor no row raises on: not 0, and not the -1 that overflows the smallest
// integer when @param minusOneOverflows
bool isSafeLiteralDivisor(Value divisor, bool minusOneOverflows) {
    ConstantOp constant = divisor.getDefiningOp<ConstantOp>();
    if (!constant) {
        return false;
    }

    const Attribute value = constant.getValue();
    if (const IntegerAttr integer = dyn_cast<IntegerAttr>(value)) {
        const llvm::APInt number = integer.getValue();
        const bool overflows = minusOneOverflows && number.isAllOnes();

        return !number.isZero() && !overflows;
    } else if (const FloatAttr real = dyn_cast<FloatAttr>(value)) {
        return !real.getValue().isZero();
    }

    return false;
}

bool runsItsBodyPerRow(Operation* op) {
    if (auto call = dyn_cast<CallSubquery>(op)) {
        return !call.getCarriesScope();
    } else if (auto exists = dyn_cast<ExistsSubquery>(op)) {
        return !exists.getCarriesScope();
    } else if (auto count = dyn_cast<CountSubquery>(op)) {
        return !count.getCarriesScope();
    } else if (auto collect = dyn_cast<CollectSubquery>(op)) {
        return !collect.getCarriesScope();
    }

    return false;
}

Value regionYield(Region& region) {
    return region.front().getTerminator()->getOperand(0);
}

LogicalResult verifyCarriedRegions(Operation* op, MutableArrayRef<Region> regions, OperandRange carried) {
    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(op);

    llvm::SmallVector<Type> forwardedTypes;
    for (size_t regionIndex = 0; regionIndex < regions.size(); regionIndex++) {
        Region& region = regions[regionIndex];
        Block& block = region.front();
        CaseYield yield = dyn_cast_or_null<CaseYield>(block.empty() ? nullptr : &block.back());

        const size_t argumentCount = forwardedTypes.size() + carried.size();
        if (!yield) {
            return op->emitOpError("region ") << regionIndex << " must end with a db.case_yield";
        } else if (block.getNumArguments() != argumentCount) {
            return op->emitOpError("region ") << regionIndex << " takes one argument per forwarded and per carried column, expected "
                                              << argumentCount << " but has " << block.getNumArguments();
        } else if (!yield.getForwarded().empty() && !carrier.forwardsColumns(region)) {
            return op->emitOpError("region ") << regionIndex << " forwards columns, but no region after it takes them";
        }

        for (size_t forwardIndex = 0; forwardIndex < forwardedTypes.size(); forwardIndex++) {
            if (block.getArgument(forwardIndex).getType() != forwardedTypes[forwardIndex]) {
                return op->emitOpError("region ") << regionIndex << " argument " << forwardIndex
                                                  << " must have the type of forwarded column " << forwardIndex;
            }
        }

        for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
            const size_t argumentIndex = forwardedTypes.size() + carriedIndex;
            if (block.getArgument(argumentIndex).getType() != carried[carriedIndex].getType()) {
                return op->emitOpError("region ") << regionIndex << " argument " << argumentIndex
                                                  << " must have the type of carried column " << carriedIndex;
            }
        }

        llvm::append_range(forwardedTypes, yield.getForwarded().getTypes());
    }

    return success();
}

// A region of a CASE or a coalesce takes the columns forwarded to it, then the carried ones
void collectSelectionArgument(Value value, ValueRange carriedColumns, llvm::SmallVectorImpl<Value>& carried) {
    BlockArgument argument = dyn_cast<BlockArgument>(value);
    if (!argument) {
        return;
    }

    Region& region = *argument.getParentRegion();
    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(region.getParentOp());

    Region* source = nullptr;
    size_t forwardIndex = 0;
    if (matchForwardedArgument(argument, source, forwardIndex)) {
        const MutableOperandRange forwarded = carrier.getForwardedColumnsMutable(*source);
        carried.push_back(forwarded[static_cast<unsigned>(forwardIndex)].get());
        return;
    }

    const size_t carriedIndex = argument.getArgNumber() - forwardedArgumentCount(region);
    carried.push_back(carriedColumns[carriedIndex]);
}

MutableOperandRange caseYieldForwards(Region& region) {
    return cast<CaseYield>(region.front().getTerminator()).getForwardedMutable();
}

bool givesNoRowAValue(Region& argument) {
    const Type element = cast<ColumnType>(regionYield(argument).getType()).getType();
    if (!isNullLiteralType(element)) {
        return false;
    }

    return llvm::all_of(argument.front().without_terminator(), [](Operation& op) {
        return ::db::runsWithoutRaising(&op);
    });
}

bool givesEveryRowAValue(Region& argument) {
    ConstantOp literal = regionYield(argument).getDefiningOp<ConstantOp>();
    if (!literal) {
        return false;
    }

    const Type element = cast<ColumnType>(literal.getResult().getType()).getType();

    return !isa<storage::NullableType, NoneType>(element) && !holdsItsOwnNull(element);
}

// Drops the regions no row takes its value out of and, where the result names its element,
// the regions behind one every row takes its value out of: lowering picks an unnamed element
// out of every region. A coalesce left with one region is that region's value where the
// value has its type.
struct SimplifyCoalesceArguments : public OpRewritePattern<Coalesce> {
    using OpRewritePattern<Coalesce>::OpRewritePattern;

    LogicalResult matchAndRewrite(Coalesce coalesce, PatternRewriter& rewriter) const override {
        const MutableArrayRef<Region> arguments = coalesce.getArguments();
        const Value result = coalesce.getResult();
        const bool namesItsElement = !isa<NoneType>(cast<ColumnType>(result.getType()).getType());

        llvm::SmallVector<size_t, 4> kept;
        for (size_t argumentIndex = 0; argumentIndex < arguments.size(); argumentIndex++) {
            Region& argument = arguments[argumentIndex];

            if (givesNoRowAValue(argument) && caseYieldForwards(argument).empty()) {
                continue;
            }

            kept.push_back(argumentIndex);

            if (namesItsElement && givesEveryRowAValue(argument)) {
                break;
            }
        }

        if (kept.empty()) {
            kept.push_back(0);
        }

        const OperandRange carried = coalesce.getColumnsToFilter();
        Region& firstKept = arguments[kept.front()];
        const bool givesWayToItsValue = kept.size() == 1 && regionYield(firstKept).getType() == result.getType();

        if (givesWayToItsValue) {
            Block& body = firstKept.front();
            Operation* const yield = body.getTerminator();

            rewriter.inlineBlockBefore(&body, coalesce, carried);

            const Value value = yield->getOperand(0);
            rewriter.eraseOp(yield);
            rewriter.replaceOp(coalesce, value);

            return success();
        } else if (kept.size() == arguments.size()) {
            return failure();
        }

        Region& lastKept = arguments[kept.back()];
        rewriter.modifyOpInPlace(lastKept.front().getTerminator(), [&lastKept] {
            caseYieldForwards(lastKept).clear();
        });

        rewriter.setInsertionPoint(coalesce);
        Coalesce trimmed = rewriter.create<Coalesce>(coalesce.getLoc(),
                                                     result.getType(),
                                                     carried,
                                                     static_cast<unsigned>(kept.size()));

        const MutableArrayRef<Region> trimmedArguments = trimmed.getArguments();
        for (size_t keptIndex = 0; keptIndex < kept.size(); keptIndex++) {
            Region& trimmedArgument = trimmedArguments[keptIndex];
            rewriter.inlineRegionBefore(arguments[kept[keptIndex]], trimmedArgument, trimmedArgument.end());
        }

        rewriter.replaceOp(coalesce, trimmed.getResult());

        return success();
    }
};

}

void Case::collectSelectedValues(llvm::SmallVectorImpl<Value>& values) {
    const OperandRange branchValues = getValues();
    values.append(branchValues.begin(), branchValues.end());

    if (const Value defaultValue = getDefaultValue()) {
        values.push_back(defaultValue);
    }
}

// db.case pairs each condition with the value the row takes when it holds, so the two
// operand groups must be parallel; a CASE with no branch is not a selection at all.
LogicalResult Case::verify() {
    const OperandRange conditions = getConditions();
    const OperandRange values = getValues();

    if (conditions.empty()) {
        return emitOpError("requires at least one branch");
    } else if (conditions.size() != values.size()) {
        return emitOpError("expects one value per condition, but has ")
               << values.size() << " values for " << conditions.size() << " conditions";
    }

    llvm::SmallVector<Value> selected;
    collectSelectedValues(selected);

    return verifySelectedResult(getOperation(), getResult(), selected);
}

void LazyCase::collectSelectedValues(llvm::SmallVectorImpl<Value>& values) {
    const MutableArrayRef<Region> branches = getBranches();

    for (size_t branchIndex = 1; branchIndex < branches.size(); branchIndex += 2) {
        values.push_back(regionYield(branches[branchIndex]));
    }

    const bool hasElse = branches.size() % 2 == 1;
    if (hasElse) {
        values.push_back(regionYield(branches.back()));
    }
}

LogicalResult LazyCase::verify() {
    const MutableArrayRef<Region> branches = getBranches();

    if (branches.size() < 2) {
        return emitOpError("requires at least one condition and its value");
    }

    return verifyCarriedRegions(getOperation(), branches, getColumnsToFilter());
}

LogicalResult LazyCase::verifyRegions() {
    llvm::SmallVector<Value> selected;
    collectSelectedValues(selected);

    return verifySelectedResult(getOperation(), getResult(), selected);
}

void Coalesce::collectSelectedValues(llvm::SmallVectorImpl<Value>& values) {
    for (Region& argument : getArguments()) {
        values.push_back(regionYield(argument));
    }
}

LogicalResult Coalesce::verify() {
    const MutableArrayRef<Region> arguments = getArguments();

    if (arguments.empty()) {
        return emitOpError("requires at least one argument");
    }

    return verifyCarriedRegions(getOperation(), arguments, getColumnsToFilter());
}

LogicalResult Coalesce::verifyRegions() {
    llvm::SmallVector<Value> selected;
    collectSelectedValues(selected);

    return verifySelectedResult(getOperation(), getResult(), selected);
}

// A nested coalesce is not asked: its answer is its own ops', which this walk reaches
// anyway, and asking it would walk them again at every level of the nesting
Speculation::Speculatability Coalesce::getSpeculatability() {
    const WalkResult walked = getOperation()->walk([](Operation* nested) {
        const bool runsWithoutRaising = isa<Coalesce>(nested) || isSpeculatable(nested);
        return runsWithoutRaising ? WalkResult::advance() : WalkResult::interrupt();
    });

    return speculatableWhen(!walked.wasInterrupted());
}

void Coalesce::getCanonicalizationPatterns(RewritePatternSet& patterns, MLIRContext* context) {
    patterns.add<SimplifyCoalesceArguments>(context);
}

LogicalResult EqOp::verify() {
    const Type lhsElement = cast<ColumnType>(getLhs().getType()).getType();
    const Type rhsElement = cast<ColumnType>(getRhs().getType()).getType();

    return verifyComparesNoNullLiteral(getOperation(), lhsElement, rhsElement);
}

LogicalResult NeqOp::verify() {
    const Type lhsElement = cast<ColumnType>(getLhs().getType()).getType();
    const Type rhsElement = cast<ColumnType>(getRhs().getType()).getType();

    return verifyComparesNoNullLiteral(getOperation(), lhsElement, rhsElement);
}

LogicalResult ToNullable::verify() {
    const Type operandType = cast<ColumnType>(getOperand().getType()).getType();
    const Type resultType = cast<ColumnType>(getResult().getType()).getType();
    const auto nullable = dyn_cast<storage::NullableType>(resultType);

    if (!nullable || nullable.getValueType() != operandType) {
        return emitOpError("must produce the nullable column of its operand's values");
    }

    return success();
}

LogicalResult CheckedCast::verify() {
    const Type resultElement = cast<ColumnType>(getResult().getType()).getType();
    if (!isa<storage::NodeIDType, storage::EdgeIDType>(resultElement)) {
        return emitOpError("must produce a node or an edge column");
    }

    const Type operandElement = columnElement(getOperand());
    if (!isa<storage::AnyType, NoneType>(operandElement) && operandElement != resultElement) {
        return emitOpError("reads tagged cells, a column whose type lowering resolves, the null literal, "
                           "or the entities it produces");
    }

    return success();
}

OpFoldResult ConstantOp::fold(FoldAdaptor adaptor) {
    return getValue();
}

// Allows inline declaration of a constant type
LogicalResult ConstantOp::inferReturnTypes(MLIRContext* context,
                                           std::optional<Location> location,
                                           ValueRange operands,
                                           DictionaryAttr attributes,
                                           PropertyRef properties,
                                           RegionRange regions,
                                           SmallVectorImpl<Type>& inferredReturnTypes) {
    ConstantOpGenericAdaptor adaptor(operands, attributes, properties, regions);

    // A list, an embedding and a map carry no type of their own, so literalType reads one off
    // the attribute's kind, and off the elements for a list
    if (llvm::isa<ArrayAttr, mlir::DenseF32ArrayAttr, mlir::DictionaryAttr>(adaptor.getValue())) {
        const mlir::Type valueType = ::db::literalType(adaptor.getValue());

        inferredReturnTypes.emplace_back(mlir::db::ColumnType::get(context, valueType));
        return success();
    }

    // Type inference runs while the op is still being parsed, ahead of the operand
    // constraint that limits the value to those three kinds, so an attribute of any other
    // kind has to be turned away here rather than cast blindly.
    const mlir::TypedAttr typedValue = llvm::dyn_cast<mlir::TypedAttr>(adaptor.getValue());
    if (!typedValue) {
        return mlir::emitOptionalError(location,
                                       "db.constant carries a typed value, a dense f32 array "
                                       "or an array of literals, not ", adaptor.getValue());
    }

    inferredReturnTypes.emplace_back(mlir::db::ColumnType::get(context, typedValue.getType()));
    return mlir::success();
}

LogicalResult RowConstant::verify() {
    const auto argument = dyn_cast<BlockArgument>(getRow());
    Operation* const body = argument ? argument.getOwner()->getParentOp() : nullptr;

    if (!body || !runsItsBodyPerRow(body)) {
        return emitOpError("operand must be an argument of a body run once per row");
    }

    return success();
}

Speculation::Speculatability CheckedCast::getSpeculatability() {
    return speculatableOverKnownTypes(getOperation()->getOperands());
}

void BroadcastConstant::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    carried.push_back(getValue());
}

void RowConstant::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    carried.push_back(getRow());
}

void ToNullable::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    carried.push_back(getOperand());
}

void Case::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    collectSelectedValues(carried);
}

void LazyCase::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    if (isa<OpResult>(value)) {
        collectSelectedValues(carried);
    }

    collectSelectionArgument(value, getColumnsToFilter(), carried);
}

bool LazyCase::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    fillTrailingRegionCarrySet(getOperation(), region, getColumnsToFilter().size(), carrySet);
    return true;
}

bool LazyCase::forwardsColumns(Region& region) {
    const size_t regionIndex = region.getRegionNumber();

    return regionIndex % 2 == 0 && regionIndex + 1 < getBranches().size();
}

MutableOperandRange LazyCase::getForwardedColumnsMutable(Region& region) {
    return caseYieldForwards(region);
}

void Coalesce::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    if (isa<OpResult>(value)) {
        collectSelectedValues(carried);
    }

    collectSelectionArgument(value, getColumnsToFilter(), carried);
}

bool Coalesce::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    fillTrailingRegionCarrySet(getOperation(), region, getColumnsToFilter().size(), carrySet);
    return true;
}

bool Coalesce::forwardsColumns(Region& region) {
    return region.getRegionNumber() + 1 < getArguments().size();
}

MutableOperandRange Coalesce::getForwardedColumnsMutable(Region& region) {
    return caseYieldForwards(region);
}

bool AndOp::isSpeculatableOver(Value lhs, Value rhs) {
    return !mayHoldAnyType(lhs) && !mayHoldAnyType(rhs);
}

Speculation::Speculatability AndOp::getSpeculatability() {
    return speculatableWhen(isSpeculatableOver(getLhs(), getRhs()));
}

Speculation::Speculatability OrOp::getSpeculatability() {
    return speculatableOverKnownTypes(getOperands());
}

Speculation::Speculatability XorOp::getSpeculatability() {
    return speculatableOverKnownTypes(getOperands());
}

Speculation::Speculatability NotOp::getSpeculatability() {
    Operation* const operation = getOperation();

    return speculatableOverKnownTypes(operation->getOperands());
}

Speculation::Speculatability AddOp::getSpeculatability() {
    return speculatableOverNumbers(getOperation());
}

// '||' raises on a value that is neither a string nor a list
Speculation::Speculatability ConcatOp::getSpeculatability() {
    return speculatableOverKnownTypes(getOperands());
}

Speculation::Speculatability SubOp::getSpeculatability() {
    return speculatableOverNumbers(getOperation());
}

Speculation::Speculatability MulOp::getSpeculatability() {
    return speculatableOverNumbers(getOperation());
}

Speculation::Speculatability DivOp::getSpeculatability() {
    return speculatableWhen(isNumberColumn(getLhs()) && isSafeLiteralDivisor(getRhs(), true));
}

Speculation::Speculatability ModOp::getSpeculatability() {
    return speculatableWhen(isNumberColumn(getLhs()) && isSafeLiteralDivisor(getRhs(), false));
}

Speculation::Speculatability PowOp::getSpeculatability() {
    return speculatableOverNumbers(getOperation());
}

double StartsWithOp::getRowCost() {
    return STRING_TEST_ROW_COST;
}

double EndsWithOp::getRowCost() {
    return STRING_TEST_ROW_COST;
}

double ContainsOp::getRowCost() {
    return STRING_TEST_ROW_COST;
}

double InOp::getRowCost() {
    return MEMBERSHIP_TEST_ROW_COST;
}
