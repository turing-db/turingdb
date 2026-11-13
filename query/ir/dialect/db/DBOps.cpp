#include "DBOps.h"
#include "DBDialect.h"

#include <optional>

#include "mlir/IR/PatternMatch.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "columns/CharacterArgument.h"

#include "DBColumnElement.h"
#include "DBHeldTags.h"
#include "IRConstantColumn.h"
#include "IRLiteralList.h"
#include "IRRange.h"
#include "IRValueTypes.h"
#include "StorageEnums.h"
#include "ColumnIndicesFormat.h"
#include "EdgeDirectionsFormat.h"
#include "PathStepsFormat.h"
#include "PathHopArguments.h"
#include "LabelAlternativesFormat.h"
#include "StorageAttributes.h"
#include "MergePatternShape.h"
#include "MakePathShape.h"
#include "NullLiteralType.h"
#include "GroupAggregateKindsFormat.h"
#include "PropertyScanLiteral.h"
#include "BoundEntityIDs.h"
#include "BoundValueFormat.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

namespace {

constexpr double STORAGE_READ_ROW_COST = 10.0;
constexpr double STRING_TEST_ROW_COST = 3.0;
constexpr double MEMBERSHIP_TEST_ROW_COST = 5.0;
constexpr double CONTAINER_BUILD_ROW_COST = 15.0;
constexpr double VECTOR_FUNCTION_ROW_COST = 40.0;

Type columnElement(Value column) {
    return ::db::declaredColumnElement(column);
}

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

Speculation::Speculatability speculatableWhen(bool cannotRaise) {
    return cannotRaise ? Speculation::Speculatable : Speculation::NotSpeculatable;
}

// Over numbers alone: a duration or an instant overflows, and a column of an unresolved
// type may hold either
Speculation::Speculatability speculatableOverNumbers(Operation* op) {
    return speculatableWhen(llvm::all_of(op->getOperands(), isNumberColumn));
}

Speculation::Speculatability speculatableWhenReadingPoints(Operation* op) {
    const bool readsPoints = llvm::all_of(op->getOperands(), [](Value operand) {
        return isa<storage::PointType>(columnElement(operand));
    });

    return speculatableWhen(readsPoints);
}

// A tagged cell, or a column whose type lowering resolves, may hold a value of any type. The
// null literal is a nullable none.
bool mayHoldAnyType(Value operand) {
    const ColumnType column = cast<ColumnType>(operand.getType());
    const Type element = columnElement(operand);
    const bool unresolved = isa<NoneType>(column.getType()) && isa<NoneType>(element);

    return unresolved || isa<storage::AnyType>(element);
}

bool writesAMapPerRow(Value column) {
    const Type declared = cast<ColumnType>(column.getType()).getType();

    return mayHoldAnyType(column) || isa<storage::MapType>(columnElement(column)) || isNullLiteralType(declared);
}

template <typename IDType>
bool holdsIDsOf(Value column) {
    return mayHoldAnyType(column) || isa<IDType>(columnElement(column));
}

// An op reading values of some types raises on a value of another, which such an operand may hold
Speculation::Speculatability speculatableOverKnownTypes(ValueRange operands) {
    return speculatableWhen(llvm::none_of(operands, mayHoldAnyType));
}

template <typename... ElementTypes>
Speculation::Speculatability speculatableOverElement(Value input) {
    return speculatableWhen(isa<ElementTypes...>(columnElement(input)));
}

bool matchIntegerLiteral(Value column, int64_t& value) {
    ConstantOp constant = column.getDefiningOp<ConstantOp>();
    const IntegerAttr integer = constant ? dyn_cast<IntegerAttr>(constant.getValue()) : IntegerAttr();
    if (!integer) {
        return false;
    }

    value = integer.getValue().getSExtValue();
    return true;
}

bool isCharacterLiteral(Value argument) {
    int64_t value = 0;

    return matchIntegerLiteral(argument, value) && value >= 0 && value <= ::db::MAX_CHARACTER_ARGUMENT;
}

// A string function counting characters raises on a cell holding no string, and on a count
// that is null, negative or past the largest it takes. Its first operand is the string, every
// other one a count
Speculation::Speculatability speculatableOverCharacters(Operation* op) {
    const bool readsAString = isa<storage::StringType>(columnElement(op->getOperand(0)));
    const bool countsLiterals = llvm::all_of(op->getOperands().drop_front(), isCharacterLiteral);

    return speculatableWhen(readsAString && countsLiterals);
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

Value yieldedFactorColumn(Region& leftFactor, Region& rightFactor, size_t resultNumber) {
    Yield leftYield = cast<Yield>(leftFactor.front().getTerminator());
    const OperandRange leftColumns = leftYield.getColumns();
    if (resultNumber < leftColumns.size()) {
        return leftColumns[resultNumber];
    }

    Yield rightYield = cast<Yield>(rightFactor.front().getTerminator());

    return rightYield.getColumns()[resultNumber - leftColumns.size()];
}

size_t factorResultOffset(Region& leftFactor, Region& factor) {
    if (&factor == &leftFactor) {
        return 0;
    }

    Yield leftYield = cast<Yield>(leftFactor.front().getTerminator());

    return leftYield.getColumns().size();
}

void fillTrailingRegionCarrySet(Operation* op, Region& region, size_t count, RegionCarrySet& carrySet) {
    carrySet = RegionCarrySet {._operandOffset = op->getNumOperands() - count,
                               ._argumentOffset = region.getNumArguments() - count,
                               ._count = count};
}

constexpr size_t HOP_FIXED_RESULT_COUNT = 4;
constexpr size_t PATH_FIXED_RESULT_COUNT = 3;

void collectCarriedResult(Value value, size_t firstCarriedResult, ValueRange carriedColumns, llvm::SmallVectorImpl<Value>& carried) {
    const OpResult result = dyn_cast<OpResult>(value);
    if (!result || result.getResultNumber() < firstCarriedResult) {
        return;
    }

    const size_t carriedIndex = result.getResultNumber() - firstCarriedResult;
    if (carriedIndex < carriedColumns.size()) {
        carried.push_back(carriedColumns[carriedIndex]);
    }
}

void collectCarriedArgument(Value value, ValueRange carriedColumns, llvm::SmallVectorImpl<Value>& carried) {
    const BlockArgument argument = dyn_cast<BlockArgument>(value);
    if (argument && argument.getArgNumber() < carriedColumns.size()) {
        carried.push_back(carriedColumns[argument.getArgNumber()]);
    }
}

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

void collectBranchYields(Value value, MutableArrayRef<Region> branches, llvm::SmallVectorImpl<Value>& carried) {
    const OpResult result = dyn_cast<OpResult>(value);
    if (!result) {
        return;
    }

    for (Region& branch : branches) {
        Yield yield = cast<Yield>(branch.front().getTerminator());
        carried.push_back(yield.getColumns()[result.getResultNumber()]);
    }
}

bool holdsOnlyKeyedValues(Value column) {
    return ::db::holdsOnlyNullOr(column, {::db::CellTag::MapView, ::db::CellTag::NodeID, ::db::CellTag::EdgeID});
}

bool holdsOnlyLists(Value column) {
    return ::db::holdsOnlyNullOr(column, {::db::CellTag::ListView});
}

bool holdsOnlyIntegers(Value column) {
    return ::db::holdsOnlyNullOr(column, {::db::CellTag::Int, ::db::CellTag::UInt});
}

// What a filter reads as a mask: a boolean column, a boolean literal, one whose values may be
// null, a property read whose type lowering resolves, or the untyped null an expression over a
// property no row carries evaluates to
bool isMaskColumn(Type type) {
    const auto column = dyn_cast<ColumnType>(type);
    if (!column) {
        return false;
    }

    const Type element = column.getType();
    if (isa<storage::BoolType>(element) || isa<NoneType>(element) || element.isInteger(1)) {
        return true;
    }

    const auto nullable = dyn_cast<storage::NullableType>(element);
    if (!nullable) {
        return false;
    }

    const Type value = nullable.getValueType();

    return isa<storage::BoolType>(value) || isa<NoneType>(value) || value.isInteger(1);
}

// Whether @param type is a column of @param Element, nullable or not, or of a cell typed at
// run time
template <typename Element>
bool isColumnOf(Type type) {
    const auto column = dyn_cast<ColumnType>(type);
    if (!column) {
        return false;
    }

    const Type element = column.getType();
    const auto nullable = dyn_cast<storage::NullableType>(element);
    const Type value = nullable ? nullable.getValueType() : element;

    return isa<Element, storage::AnyType, NoneType>(value);
}

// A point function reads columns of points into a column of @param Result
template <typename Result>
LogicalResult verifyPointFunction(Operation* op, Value result, StringRef resultName) {
    const bool readsPoints = llvm::all_of(op->getOperands(), [](Value operand) {
        return isColumnOf<storage::PointType>(operand.getType());
    });

    if (!readsPoints) {
        return op->emitOpError("operands must be point columns");
    }

    const ColumnType resultColumn = dyn_cast<ColumnType>(result.getType());
    if (!resultColumn || !isa<Result>(resultColumn.getType())) {
        return op->emitOpError("result must be a column of ") << resultName;
    }

    return success();
}

bool isIntegerColumn(Value column) {
    return isa<ColumnType>(column.getType())
        && isa<IntegerType, storage::AnyType, NoneType>(::db::declaredColumnElement(column));
}

LogicalResult verifyStringResult(Operation* op, Value result) {
    const ColumnType resultColumn = dyn_cast<ColumnType>(result.getType());
    if (!resultColumn || !isa<storage::StringType>(resultColumn.getType())) {
        return op->emitOpError("result must be a column of strings");
    }

    return success();
}

LogicalResult verifyStringEnd(Operation* op, Value string, Value length, Value result) {
    const bool readsAStringAndALength = isColumnOf<storage::StringType>(string.getType()) && isIntegerColumn(length);
    if (!readsAStringAndALength) {
        return op->emitOpError("operands must be a string column and an integer column");
    }

    return verifyStringResult(op, result);
}

LogicalResult verifyEdgeTypesNotEmpty(Operation* operation, ArrayAttr edgeTypes) {
    if (edgeTypes.empty()) {
        return operation->emitOpError("requires at least one edge type");
    }

    return success();
}

// One list of names per step of a path exploration, an empty list leaving its step free
LogicalResult verifyStepNames(Operation* operation, ArrayAttr names, size_t stepCount, llvm::StringRef what) {
    if (!names) {
        return success();
    } else if (names.size() != stepCount) {
        return operation->emitOpError(what) << " must hold one list per step";
    }

    bool namesAny = false;
    for (const Attribute stepNames : names) {
        for (const Attribute name : cast<ArrayAttr>(stepNames)) {
            const StringAttr text = dyn_cast<StringAttr>(name);
            if (text && text.getValue().empty()) {
                return operation->emitOpError(what) << " must not hold an empty name";
            }

            namesAny = true;
        }
    }

    if (!namesAny) {
        return operation->emitOpError(what) << " must name at least one name";
    }

    return success();
}

// The names of one step of a path exploration, null when the step leaves them free
ArrayAttr stepNamesAt(ArrayAttr names, size_t step) {
    if (!names) {
        return {};
    }

    const ArrayAttr stepNames = cast<ArrayAttr>(names[step]);

    return stepNames.empty() ? ArrayAttr() : stepNames;
}

// The keyword that introduces each factor region in the textual form
// `db.cross_product factor { ... } factor { ... }`.
const char* const factorKeyword = "factor";

// The keyword db.hash_join spells its two key column indices after.
const char* const keysKeyword = "on";

// The db.output a union branch ends with, or a null Output when the branch is empty or
// ends on something else. A branch's output is what fills the union's result table.
Output getBranchOutput(Region& branch) {
    Operation* last = nullptr;
    if (!branch.empty()) {
        Block& block = branch.front();
        if (!block.empty()) {
            last = &block.back();
        }
    }

    return dyn_cast_or_null<Output>(last);
}

// The db.yield that terminates a factor region, or a null Yield if the region
// is empty or does not end with one. A factor's yield names the columns that
// factor contributes to the product.
Yield getFactorYield(Region& factor) {
    Operation* terminator = nullptr;
    if (!factor.empty()) {
        Block& block = factor.front();
        if (!block.empty()) {
            terminator = &block.back();
        }
    }

    return dyn_cast_or_null<Yield>(terminator);
}

// A branch feeding the results of the op holding it, rather than the result table, ends
// in a db.yield naming one column per result
LogicalResult verifyYieldingBranch(Operation* holder, Region& branch) {
    Yield yield = getFactorYield(branch);
    if (!yield) {
        return holder->emitOpError("each branch must end with a db.yield");
    }

    if (yield.getColumns().size() != holder->getNumResults()) {
        return holder->emitOpError("each branch must yield one column per result");
    }

    return success();
}

// Appends the columns yielded by a factor region to resultTypes, failing with a
// diagnostic if the factor is not terminated by a db.yield.
ParseResult appendFactorYieldTypes(OpAsmParser& parser,
                                   Region& factor,
                                   llvm::SmallVectorImpl<Type>& resultTypes) {
    Yield yield = getFactorYield(factor);
    if (!yield) {
        return parser.emitError(parser.getCurrentLocation(),
                                "factor must end with a db.yield");
    }

    for (const Type columnType : yield.getColumns().getTypes()) {
        resultTypes.push_back(columnType);
    }

    return success();
}

// The body of an op over the elements of one row's list: one block taking the element,
// the row tag and one argument per carried column, ending in a yield that names the tag
LogicalResult verifyElementBody(Operation* op, Block& bodyBlock, OperandRange carried) {
    auto yield = dyn_cast_or_null<ComprehensionYield>(bodyBlock.empty() ? nullptr : &bodyBlock.back());
    if (!yield) {
        return op->emitOpError("body region must end with a db.comprehension_yield");
    }

    const size_t expectedArguments = carried.size() + 2;

    if (bodyBlock.getNumArguments() != expectedArguments) {
        return op->emitOpError("body region takes the element and the row tag plus one argument per "
                               "carried column, ")
               << "expected " << expectedArguments << " but has " << bodyBlock.getNumArguments();
    }

    if (!llvm::isa<ColumnType>(bodyBlock.getArgument(0).getType())) {
        return op->emitOpError("body argument 0 must be the column of elements");
    }

    const auto rowTagType = llvm::dyn_cast<ColumnType>(bodyBlock.getArgument(1).getType());

    if (!rowTagType || !rowTagType.getType().isUnsignedInteger(64)) {
        return op->emitOpError("body argument 1 must be the ui64 column of row tags");
    }

    if (!yield.getRowTags()) {
        return op->emitOpError("body must yield the row tag of every surviving element");
    }

    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        const mlir::Type argumentType = bodyBlock.getArgument(carriedIndex + 2).getType();

        if (argumentType != carried[carriedIndex].getType()) {
            return op->emitOpError("body argument ") << carriedIndex + 2
                                                     << " must have the type of carried column "
                                                     << carriedIndex;
        }
    }

    return success();
}

// A region of a fold over the elements of one row's list: one block taking the element,
// the accumulator and one argument per carried column, ending in a @param YieldOp
template <typename YieldOp>
LogicalResult verifyFoldRegion(Operation* op, Block& block, OperandRange carried, llvm::StringLiteral yieldName) {
    if (block.empty() || !isa<YieldOp>(block.back())) {
        return op->emitOpError("region must end with a ") << yieldName;
    }

    const size_t expectedArguments = carried.size() + 2;

    if (block.getNumArguments() != expectedArguments) {
        return op->emitOpError("region takes the element and the accumulator plus one argument per "
                               "carried column, ")
               << "expected " << expectedArguments << " but has " << block.getNumArguments();
    }

    for (size_t argumentIndex = 0; argumentIndex < 2; argumentIndex++) {
        if (!llvm::isa<ColumnType>(block.getArgument(argumentIndex).getType())) {
            return op->emitOpError("argument ") << argumentIndex << " must be a column";
        }
    }

    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        const mlir::Type argumentType = block.getArgument(carriedIndex + 2).getType();

        if (argumentType != carried[carriedIndex].getType()) {
            return op->emitOpError("argument ") << carriedIndex + 2
                                                << " must have the type of carried column "
                                                << carriedIndex;
        }
    }

    return success();
}

// Parses `factor { ... }` into a fresh region of result. The factor takes no
// operands and no block arguments, so the region is parsed with an empty
// argument list.
ParseResult parseFactorRegion(OpAsmParser& parser, OperationState& result) {
    if (parser.parseKeyword(factorKeyword)) {
        return failure();
    }

    Region* factor = result.addRegion();
    return parser.parseRegion(*factor, {});
}

// The results of a two-factor op - db.cross_product, db.hash_join - must be exactly the
// columns the two factors yield: the left factor's yielded columns followed by the right
// factor's. The yields drive the result types during parsing, so this guards the
// programmatic builder path and re-checks parsed IR. Shared by both verifiers.
LogicalResult verifyFactorResults(Operation* op, Region& leftFactor, Region& rightFactor) {
    Yield leftYield = getFactorYield(leftFactor);
    Yield rightYield = getFactorYield(rightFactor);
    if (!leftYield || !rightYield) {
        return op->emitOpError("each factor region must end with a db.yield");
    }

    // Each factor must contribute at least one column. A side's row count is read
    // from its first yielded column during lowering, so a factor that surfaces no
    // column (an empty db.yield) cannot be sized - reject it here at the db level.
    if (leftYield.getColumns().empty() || rightYield.getColumns().empty()) {
        return op->emitOpError("each factor must yield at least one column");
    }

    llvm::SmallVector<Type> expectedResultTypes;
    for (const Type columnType : leftYield.getColumns().getTypes()) {
        expectedResultTypes.push_back(columnType);
    }
    for (const Type columnType : rightYield.getColumns().getTypes()) {
        expectedResultTypes.push_back(columnType);
    }

    const Operation::result_type_range resultTypes = op->getResultTypes();
    if (resultTypes.size() != expectedResultTypes.size()) {
        return op->emitOpError("expects ") << expectedResultTypes.size()
                                           << " results, the columns yielded by the two factors, but has "
                                           << resultTypes.size();
    }

    for (size_t resultIndex = 0; resultIndex < expectedResultTypes.size(); resultIndex++) {
        if (resultTypes[resultIndex] != expectedResultTypes[resultIndex]) {
            return op->emitOpError("result ") << resultIndex << " must be the yielded column type "
                                              << expectedResultTypes[resultIndex];
        }
    }

    return success();
}

// A literal list typed as homogeneous - db.unwind_const's typed column, db.const_list's
// typed list - must carry an element naming a type, and its elements' types must join to
// one element type; a null rides a unit attr and joins whatever the others carry.
// The elements are checked against each other and not against the spelled element type: a
// hand-written "s" parses as an untyped StringAttr, so comparing it to a !storage.string
// would reject valid IR. A homogeneous result paired with literals of another type is
// therefore not an op-level error; the runtime fill catches that on the element's type tag.
LogicalResult verifyHomogeneousElements(Operation* op, ArrayAttr elements) {
    const mlir::Type sharedType = ::db::sharedLiteralElementType(elements);

    if (isa<NoneType>(sharedType)) {
        return op->emitOpError("a homogeneous literal list must carry an element naming its type; "
                               "an empty list or a list of nulls alone is the !storage.any form");
    } else if (isa<storage::AnyType>(sharedType)) {
        return op->emitOpError("a homogeneous literal list requires every element to share one type");
    }

    return success();
}

// db.limit and db.skip both pass their columns straight through, so the results
// must be exactly the input columns - same count, same types and in the same
// order. Their row count is read from the first column during lowering, so a
// pass-through over no column cannot be sized and is rejected here at the db
// level. Shared by both verifiers.
LogicalResult verifyPassThrough(Operation* op,
                                OperandRange columns,
                                Operation::result_range results) {
    if (columns.empty()) {
        return op->emitOpError("requires at least one column");
    }

    if (columns.size() != results.size()) {
        return op->emitOpError("expects ") << columns.size()
                                           << " results, one per input column, but has "
                                           << results.size();
    }

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (columns[columnIndex].getType() != results[columnIndex].getType()) {
            return op->emitOpError("result ") << columnIndex
                                              << " must have the same type as input column "
                                              << columnIndex;
        }
    }

    return success();
}

// The count of a db.limit or a db.skip: the literal, or an integer bound for the whole run, or
// a cell holding one, which the loop checks when it starts
LogicalResult verifyCutCount(Operation* op, std::optional<uint64_t> count, Value boundCount) {
    if (count.has_value() == static_cast<bool>(boundCount)) {
        return op->emitOpError("requires either a count or a bound one");
    } else if (!boundCount) {
        return success();
    }

    const Type element = columnElement(boundCount);
    if (!element.isSignlessInteger(64) && !isa<NoneType, storage::AnyType>(element)) {
        return op->emitOpError("requires a bound count of type i64 or !storage.any, got ") << element;
    } else if (!::db::readsNoRow(boundCount)) {
        return op->emitOpError("requires a bound count computed from parameters and literals alone");
    }

    return success();
}

// A value a db.vector_search reads when it starts: one value for every row the search is
// paired with, computed from parameters and literals or read off the row a body runs for.
// A cell, or an element lowering resolves, is checked when the search starts.
LogicalResult verifySearchedValue(Operation* op, llvm::StringRef name, Value bound, Type element) {
    if (!bound) {
        return success();
    }

    const Type boundElement = columnElement(bound);
    if (boundElement != element && !isa<NoneType, storage::AnyType>(boundElement)) {
        return op->emitOpError("requires a bound ") << name << " of type " << element << " or !storage.any, got " << boundElement;
    } else if (!::db::readsNoRow(bound) && !::db::yieldsConstantColumn(bound)) {
        return op->emitOpError("requires a bound ") << name << " holding one value for every row";
    }

    return success();
}

// The hop region of one step of an explore_paths: one block over the nodes and edges of the
// repetition up to the step's end node yielding a boolean column, reading nothing defined
// outside it but constants
LogicalResult verifyHopRegion(Operation* op, Region& hop, size_t step, ValueRange imports) {
    Block& block = hop.front();
    MLIRContext* context = op->getContext();

    const Type nodeColumn = ColumnType::get(context, storage::NodeIDType::get(context));
    const Type edgeColumn = ColumnType::get(context, storage::EdgeIDType::get(context));
    if (failed(verifyHopArguments(op, block, step, nodeColumn, edgeColumn, imports))) {
        return failure();
    }

    Yield yield = getFactorYield(hop);
    if (!yield) {
        return op->emitOpError("hop region must end with a db.yield");
    }

    const bool yieldsOneMask = yield.getColumns().size() == 1 && isMaskColumn(yield.getColumns().front().getType());
    if (!yieldsOneMask) {
        return op->emitOpError("hop region must yield exactly one boolean column");
    }

    for (Operation& inner : block) {
        for (const Value operand : inner.getOperands()) {
            if (const auto argument = dyn_cast<BlockArgument>(operand)) {
                if (argument.getOwner() != &block && !::db::isFunctionArgument(argument)) {
                    return op->emitOpError("hop region reads a block argument of another region");
                }

                continue;
            }

            Operation* const definingOp = operand.getDefiningOp();
            const bool definedInside = definingOp->getBlock() == &block;
            if (!definedInside && !::db::yieldsConstantColumn(operand)) {
                return op->emitOpError("hop region may only read constants from outside itself");
            }
        }
    }

    return success();
}

// A body run once per row holds one value per argument for the whole run, and reads it as
// the constant it is: read directly, an argument would be a column of its own rows
LogicalResult verifyArgumentsReadAsRunConstants(Operation* subquery, Block& body) {
    for (BlockArgument argument : body.getArguments()) {
        for (Operation* user : argument.getUsers()) {
            if (!isa<RowConstant>(user)) {
                return subquery->emitOpError("a body run once per row reads argument ")
                       << argument.getArgNumber() << " through db.row_constant, not through "
                       << user->getName();
            }
        }
    }

    return success();
}

template <typename YieldOp, typename SubqueryOp>
LogicalResult verifySubqueryExpression(SubqueryOp subquery) {
    Block& bodyBlock = subquery.getBody().front();

    auto yield = dyn_cast_or_null<YieldOp>(bodyBlock.empty() ? nullptr : &bodyBlock.back());
    if (!yield) {
        return subquery.emitOpError("body region must end with a ") << YieldOp::getOperationName();
    }

    const OperandRange inputs = subquery.getInputColumns();
    const size_t inputCount = inputs.size();

    // The body carries the row tag through its dataflow only when it carries the scope,
    // and only over rows it has: a body run one row at a time, or over the single empty
    // row, is answered for by the lowering instead.
    const bool tagsRows = subquery.getCarriesScope() && inputCount > 0;

    const size_t expectedArguments = tagsRows ? inputCount + 1 : inputCount;
    const size_t argumentCount = bodyBlock.getNumArguments();

    if (argumentCount != expectedArguments) {
        return subquery.emitOpError("body region takes one argument per input column")
               << (tagsRows ? " plus the row tag" : "") << ", expected " << expectedArguments
               << " but has " << argumentCount;
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        if (bodyBlock.getArgument(inputIndex).getType() != inputs[inputIndex].getType()) {
            return subquery.emitOpError("body argument ") << inputIndex << " must have the type of input column "
                                                          << inputIndex;
        }
    }

    const bool yieldsATag = yield.getTag() != nullptr;
    if (tagsRows != yieldsATag) {
        return subquery.emitOpError("the body yields a row tag exactly when it takes one");
    }

    // A body with no tag is answered for by the rows its columns hold, so it has to hold
    // one; a tagged body is answered for by the tag, and holds whatever its clauses left.
    if (!tagsRows && yield.getColumns().empty()) {
        return subquery.emitOpError("an untagged body must yield at least one column, to answer from its rows");
    }

    if (!subquery.getCarriesScope()) {
        return verifyArgumentsReadAsRunConstants(subquery.getOperation(), bodyBlock);
    }

    return success();
}

// A carried column comes back replicated, never retyped, so the carry set and the
// results past the element column must match one for one.
LogicalResult verifyCarriedResults(Operation* op, OperandRange carriedColumns, ResultRange carriedResults) {
    if (carriedColumns.size() != carriedResults.size()) {
        return op->emitOpError("expects one carried result per carried column, but has ")
               << carriedResults.size() << " for " << carriedColumns.size();
    }

    for (size_t carriedIndex = 0; carriedIndex < carriedColumns.size(); carriedIndex++) {
        if (carriedColumns[carriedIndex].getType() != carriedResults[carriedIndex].getType()) {
            return op->emitOpError("carried result ") << carriedIndex
                                                      << " must have the same type as carried column "
                                                      << carriedIndex;
        }
    }

    return success();
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

#define GET_OP_CLASSES
#include "DBOps.cpp.inc"

// Ensures each variable has a numeric name
void ScanEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ConstScanEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void FetchEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanEdgesByType::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanOutEdgesByLabelSrc::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanInEdgesByLabelTgt::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanOutEdgesByLabelTgt::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanInEdgesByLabelSrc::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanOutEdgesByLabelSrcTgt::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanInEdgesByLabelSrcTgt::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

// Ensures each variable has a numeric name
void GetOutEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ExplorePaths::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

storage::PathDirection ExplorePaths::getStepDirection(size_t step) {
    return static_cast<storage::PathDirection>(getDirections()[step]);
}

ArrayAttr ExplorePaths::getStepEdgeTypes(size_t step) {
    return stepNamesAt(getEdgeTypesAttr(), step);
}

ArrayAttr ExplorePaths::getStepHopLabels(size_t step) {
    return stepNamesAt(getHopLabelsAttr(), step);
}

LogicalResult ExplorePaths::verify() {
    const OperandRange carried = getColumnsToFilter();
    const ResultRange filtered = getFilteredColumns();
    if (carried.size() != filtered.size()) {
        return emitOpError("expects one filtered column per carried column, but carries ")
               << carried.size() << " and filters " << filtered.size();
    }

    for (size_t columnIndex = 0; columnIndex < carried.size(); columnIndex++) {
        if (carried[columnIndex].getType() != filtered[columnIndex].getType()) {
            return emitOpError("filtered column ") << columnIndex
                                                   << " must have the type of carried column "
                                                   << columnIndex;
        }
    }

    const std::optional<uint64_t> maxHops = getMaxHops();
    if (maxHops && *maxHops < getMinHops()) {
        return emitOpError("max_hops must be at least min_hops");
    }

    MutableArrayRef<Region> hops = getHops();
    if (failed(verifyPathSteps(getOperation(), getDirections(), hops.size()))) {
        return failure();
    }

    const size_t stepCount = getStepCount();
    const bool stepNamesFail = failed(verifyStepNames(getOperation(), getEdgeTypesAttr(), stepCount, "edge_types"))
                            || failed(verifyStepNames(getOperation(), getHopLabelsAttr(), stepCount, "hop_labels"));
    if (stepNamesFail) {
        return failure();
    }

    if (const std::optional<ArrayAttr> endLabels = getEndLabels()) {
        if (endLabels->empty()) {
            return emitOpError("end_labels must name at least one label");
        }

        for (const Attribute label : *endLabels) {
            const StringAttr name = dyn_cast<StringAttr>(label);
            if (name && name.getValue().empty()) {
                return emitOpError("end_labels must name labels");
            }
        }
    }

    if (const std::optional<uint64_t> endColumn = getEndColumn()) {
        if (*endColumn >= carried.size()) {
            return emitOpError("end_column ") << *endColumn << " is not a carried column";
        }

        const auto endType = cast<ColumnType>(carried[*endColumn].getType());
        if (!isa<storage::NodeIDType>(endType.getType())) {
            return emitOpError("end_column must name a node column");
        }
    }

    if (getEndsOnSeed() && getEndColumn()) {
        return emitOpError("ends_on_seed names the end already named by end_column");
    }

    if (getEndNodes() && (getEndColumn() || getEndsOnSeed())) {
        return emitOpError("end_nodes names the end already named by end_column or ends_on_seed");
    }

    if (getDistinct()) {
        if (!getPaths().use_empty()) {
            return emitOpError("distinct emits no path, so paths must have no use");
        }
    }

    const OperandRange imports = getHopImports();

    const bool hasPredicate = hasHopPredicate(hops);
    if (!hasPredicate) {
        if (!imports.empty()) {
            return emitOpError("hop imports without a hop region to read them");
        }

        return success();
    }

    if (getDistinct() && !imports.empty()) {
        return emitOpError("a distinct search expands many seeds at one level, so it cannot "
                           "read a column that holds one value per seed");
    }

    for (size_t step = 0; step < stepCount; step++) {
        Region& hop = hops[step];
        if (!hop.empty() && failed(verifyHopRegion(getOperation(), hop, step, imports))) {
            return failure();
        }
    }

    return success();
}

LogicalResult ExpandPath::verify() {
    const auto column = dyn_cast<ColumnType>(getResult().getType());
    const auto list = column ? dyn_cast<storage::ListType>(column.getType()) : storage::ListType();
    if (!list) {
        return emitOpError("must produce a list column");
    }

    const Type elementType = list.getElementType();
    const storage::PathExpansionKind kind = getKind();

    if (kind == storage::PathExpansionKind::Edges) {
        if (!isa<storage::EdgeIDType>(elementType)) {
            return emitOpError("kind edges expands to a list of edge IDs");
        }
    } else if (!isa<storage::NodeIDType>(elementType)) {
        return emitOpError("kind sources, ends and nodes expand to a list of node IDs");
    }

    const std::optional<uint64_t> step = getStep();
    const std::optional<uint64_t> steps = getSteps();
    if (step.has_value() != steps.has_value()) {
        return emitOpError("step and steps name a step of the body together");
    } else if (steps && *step >= *steps) {
        return emitOpError("step ") << *step << " is not one of the " << *steps << " steps of the body";
    } else if (steps && kind == storage::PathExpansionKind::Nodes) {
        return emitOpError("kind nodes lists every node of the walk and takes no step");
    }

    // Reversed, the kinds swap: the pattern's end list is the walk's source list read
    // backwards, so that is the one holding the seed. The nodes hold it either way.
    const bool readsTheSeed = (kind == storage::PathExpansionKind::Sources && step.value_or(0) == 0)
                           || kind == storage::PathExpansionKind::Nodes;
    if (readsTheSeed && !getSrcids()) {
        return emitOpError("kind sources and nodes read the seed of each path from srcids");
    }

    return success();
}

LogicalResult PathLength::verify() {
    const std::optional<uint64_t> steps = getSteps();
    if (steps && *steps == 0) {
        return emitOpError("steps counts the steps of a body, so it is at least 1");
    } else if (steps && getNodes()) {
        return emitOpError("nodes counts every node of the walk, which reads no step of a body");
    }

    return success();
}

LogicalResult PathElements::verify() {
    const Type path = columnElement(getPath());
    if (!isa<storage::PathType, storage::AnyType, NoneType>(path)) {
        return emitOpError("reads a path column, a column of tagged cells, or a column whose type lowering resolves");
    }

    const auto column = dyn_cast<ColumnType>(getResult().getType());
    const auto nullable = column ? dyn_cast<storage::NullableType>(column.getType()) : storage::NullableType();
    if (!nullable) {
        return emitOpError("must produce a nullable column: the path an OPTIONAL MATCH missed reads as null");
    }

    const Type value = nullable.getValueType();
    const auto list = dyn_cast<storage::ListType>(value);
    const Type element = list ? list.getElementType() : Type();

    switch (getKind()) {
        case storage::PathElementsKind::Nodes:
            if (!isa_and_nonnull<storage::NodeIDType>(element)) {
                return emitOpError("kind nodes reads a list of node IDs");
            }
        break;

        case storage::PathElementsKind::Relationships:
            if (!isa_and_nonnull<storage::EdgeIDType>(element)) {
                return emitOpError("kind relationships reads a list of edge IDs");
            }
        break;

        case storage::PathElementsKind::Length:
            if (!value.isUnsignedInteger(64)) {
                return emitOpError("kind length reads a ui64 count");
            }
        break;
    }

    return success();
}

LogicalResult MakePath::verify() {
    llvm::SmallVector<Type> elementTypes;
    for (const Value entity : getEntities()) {
        const auto column = dyn_cast<ColumnType>(entity.getType());
        elementTypes.push_back(column ? column.getType() : Type());
    }

    return verifyMakePath(getOperation(), elementTypes, getReversedPaths());
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

// Builds the op from just the result types - the left factor's yielded columns
// followed by the right factor's - and creates the two empty factor blocks. The
// caller fills each region and terminates it with a db.yield whose operands
// match the result types contributed by that factor. The insertion guard keeps
// the block creation from leaking out of the builder.
void CrossProduct::build(OpBuilder& builder, OperationState& state, TypeRange resultTypes) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addTypes(resultTypes);

    Region* leftFactor = state.addRegion();
    builder.createBlock(leftFactor);

    Region* rightFactor = state.addRegion();
    builder.createBlock(rightFactor);
}

// Custom syntax, mirroring the disconnected pattern it models:
//
//   %a, %b = db.cross_product factor { ... db.yield %x : ... }
//                             factor { ... db.yield %y : ... }
//
// Nothing is spelled after the regions: the result types are recovered from the
// two factors' yields - the left factor's yielded columns followed by the
// right factor's - matching the result count parsed from the `%a, %b =` list.
ParseResult CrossProduct::parse(OpAsmParser& parser, OperationState& result) {
    const bool regionsFailed = parseFactorRegion(parser, result)
                               || parseFactorRegion(parser, result)
                               || parser.parseOptionalAttrDict(result.attributes);
    if (regionsFailed) {
        return failure();
    }

    Region& leftFactor = *result.regions[0];
    Region& rightFactor = *result.regions[1];
    if (appendFactorYieldTypes(parser, leftFactor, result.types)
        || appendFactorYieldTypes(parser, rightFactor, result.types)) {
        return failure();
    }

    return success();
}

void CrossProduct::print(OpAsmPrinter& printer) {
    printer << " " << factorKeyword << " ";
    printer.printRegion(getLeftFactor());

    printer << " " << factorKeyword << " ";
    printer.printRegion(getRightFactor());

    printer.printOptionalAttrDict((*this)->getAttrs());
}

LogicalResult CrossProduct::verify() {
    return verifyFactorResults(getOperation(), getLeftFactor(), getRightFactor());
}

// Builds the op from the branch count alone and creates that many empty blocks. The
// caller fills each region with a query body and ends it with a db.output. The insertion
// guard keeps the block creation from leaking out of the builder, as CrossProduct's does.
void Union::build(OpBuilder& builder, OperationState& state, size_t branchCount) {
    build(builder, state, TypeRange {}, branchCount);
}

void Union::build(OpBuilder& builder, OperationState& state, TypeRange resultTypes, size_t branchCount) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addTypes(resultTypes);

    for (size_t branchIndex = 0; branchIndex < branchCount; branchIndex++) {
        Region* branch = state.addRegion();
        builder.createBlock(branch);
    }
}

// Custom syntax, one region per branch separated by commas:
//
//   db.union { ... db.output(%a) names ["name"] : ... },
//             { ... db.output(%b) names ["name"] : ... }
//
// A union with results spells their types after the regions:
//
//   %z = db.union { ... db.yield %a : ... }, { ... db.yield %b : ... } : !db.column<none>
ParseResult Union::parse(OpAsmParser& parser, OperationState& result) {
    do {
        Region* branch = result.addRegion();
        if (parser.parseRegion(*branch, {})) {
            return failure();
        }
    } while (succeeded(parser.parseOptionalComma()));

    if (succeeded(parser.parseOptionalColon())) {
        SmallVector<Type> resultTypes;
        if (parser.parseTypeList(resultTypes)) {
            return failure();
        }

        result.addTypes(resultTypes);
    }

    return parser.parseOptionalAttrDict(result.attributes);
}

void Union::print(OpAsmPrinter& printer) {
    llvm::interleave(getBranches(),
                     printer,
                     [&](Region& branch) {
                         printer << " ";
                         printer.printRegion(branch);
                     },
                     ",");

    const ResultRange results = getResults();
    if (!results.empty()) {
        printer << " : ";
        llvm::interleaveComma(results.getTypes(), printer);
    }

    printer.printOptionalAttrDict((*this)->getAttrs());
}

LogicalResult Materialize::verify() {
    if (getColumns().empty()) {
        return emitOpError("requires at least one column");
    }

    return success();
}

LogicalResult ScanMaterialized::verify() {
    Materialize materialize = getRows().getDefiningOp<Materialize>();
    if (!materialize) {
        return emitOpError("requires rows a db.materialize holds");
    }

    const OperandRange heldColumns = materialize.getColumns();
    const ResultRange columns = getColumns();
    if (columns.size() != heldColumns.size()) {
        return emitOpError("expects ") << heldColumns.size()
                                       << " results, one per column the db.materialize holds, but has "
                                       << columns.size();
    }

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (columns[columnIndex].getType() != heldColumns[columnIndex].getType()) {
            return emitOpError("result ") << columnIndex << " must have the type of the column it reads";
        }
    }

    return success();
}

// A union emits one result table, so every branch has to end in a db.output and all of
// them have to name the same columns. The names are the query's, checked by the analyzer;
// what is re-checked here is the shape a hand-written module can still get wrong.
LogicalResult Union::verify() {
    const MutableArrayRef<Region> branches = getBranches();
    if (branches.size() < 2) {
        return emitOpError("requires at least two branches");
    }

    if (getNumResults() > 0) {
        for (Region& branch : branches) {
            if (failed(verifyYieldingBranch(getOperation(), branch))) {
                return failure();
            }
        }

        return success();
    }

    Output first;
    for (Region& branch : branches) {
        Output output = getBranchOutput(branch);
        if (!output) {
            return emitOpError("each branch must end with a db.output");
        }

        if (!first) {
            first = output;
            continue;
        }

        if (output.getColumns().size() != first.getColumns().size()) {
            return emitOpError("every branch must output the same number of columns");
        }

        if (output.getColumnNamesAttr() != first.getColumnNamesAttr()) {
            return emitOpError("every branch must output the same column names");
        }
    }

    return success();
}

void Conditional::build(OpBuilder& builder,
                        OperationState& state,
                        TypeRange resultTypes,
                        size_t regionCount) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addTypes(resultTypes);

    for (size_t regionIndex = 0; regionIndex < regionCount; regionIndex++) {
        Region* region = state.addRegion();
        builder.createBlock(region);
    }
}

LogicalResult Conditional::verify() {
    const MutableArrayRef<Region> regions = getBranches();
    if (regions.size() < 2) {
        return emitOpError("requires a condition and the branch it guards");
    }

    const size_t conditionCount = regions.size() / 2;
    for (size_t conditionIndex = 0; conditionIndex < conditionCount; conditionIndex++) {
        Yield condition = getFactorYield(regions[2 * conditionIndex]);
        if (!condition || condition.getColumns().size() != 1) {
            return emitOpError("each condition must end with a db.yield of one column");
        }

        if (failed(verifyYieldingBranch(getOperation(), regions[2 * conditionIndex + 1]))) {
            return failure();
        }
    }

    const bool hasDefault = regions.size() % 2 == 1;
    if (hasDefault) {
        return verifyYieldingBranch(getOperation(), regions.back());
    }

    return success();
}

// Builds the op from the result types - the left factor's yielded columns followed
// by the right factor's - and the two key column indices, and creates the two empty
// factor blocks. The cross product's sibling builder, with the join keys added.
void HashJoin::build(OpBuilder& builder,
                     OperationState& state,
                     TypeRange resultTypes,
                     uint64_t leftKey,
                     uint64_t rightKey) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addTypes(resultTypes);

    const Type keyIndexType = builder.getIntegerType(64, /*isSigned=*/false);
    state.addAttribute(getLeftKeyAttrName(state.name), builder.getIntegerAttr(keyIndexType, leftKey));
    state.addAttribute(getRightKeyAttrName(state.name), builder.getIntegerAttr(keyIndexType, rightKey));

    Region* leftFactor = state.addRegion();
    builder.createBlock(leftFactor);

    Region* rightFactor = state.addRegion();
    builder.createBlock(rightFactor);
}

// Custom syntax, the cross product's with the join keys spelled after the regions:
//
//   %a, %ka, %b, %kb = db.hash_join factor { ... db.yield %x, %k : ... }
//                                   factor { ... db.yield %y, %k : ... } on 1, 1
//
// The result types are recovered from the two factors' yields, exactly as
// db.cross_product recovers them.
ParseResult HashJoin::parse(OpAsmParser& parser, OperationState& result) {
    if (parseFactorRegion(parser, result) || parseFactorRegion(parser, result)) {
        return failure();
    }

    uint64_t leftKey = 0;
    uint64_t rightKey = 0;
    const bool keysFailed = parser.parseKeyword(keysKeyword)
                            || parser.parseInteger(leftKey)
                            || parser.parseComma()
                            || parser.parseInteger(rightKey);
    if (keysFailed) {
        return failure();
    }

    if (parser.parseOptionalAttrDict(result.attributes)) {
        return failure();
    }

    const Type keyIndexType = parser.getBuilder().getIntegerType(64, /*isSigned=*/false);
    result.addAttribute(getLeftKeyAttrName(result.name), IntegerAttr::get(keyIndexType, leftKey));
    result.addAttribute(getRightKeyAttrName(result.name), IntegerAttr::get(keyIndexType, rightKey));

    Region& leftFactor = *result.regions[0];
    Region& rightFactor = *result.regions[1];
    if (appendFactorYieldTypes(parser, leftFactor, result.types)
        || appendFactorYieldTypes(parser, rightFactor, result.types)) {
        return failure();
    }

    return success();
}

void HashJoin::print(OpAsmPrinter& printer) {
    printer << " " << factorKeyword << " ";
    printer.printRegion(getLeftFactor());

    printer << " " << factorKeyword << " ";
    printer.printRegion(getRightFactor());

    printer << " " << keysKeyword << " " << getLeftKey() << ", " << getRightKey();

    printer.printOptionalAttrDict((*this)->getAttrs(), {getLeftKeyAttrName(), getRightKeyAttrName()});
}

// The results line up with the two factors' yields exactly as a cross product's do, and
// each key index has to name a column of its own factor's yield - it is what the join
// matches on, so a key past the end names no column to read.
LogicalResult HashJoin::verify() {
    if (failed(verifyFactorResults(getOperation(), getLeftFactor(), getRightFactor()))) {
        return failure();
    }

    const size_t leftCount = getFactorYield(getLeftFactor()).getColumns().size();
    const size_t rightCount = getFactorYield(getRightFactor()).getColumns().size();

    if (getLeftKey() >= leftCount) {
        return emitOpError("left key column ") << getLeftKey() << " is past the "
                                               << leftCount << " columns the left factor yields";
    }

    if (getRightKey() >= rightCount) {
        return emitOpError("right key column ") << getRightKey() << " is past the "
                                                << rightCount << " columns the right factor yields";
    }

    return success();
}

LogicalResult CreateNode::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    if (getPropNames().size() != getPropValues().size()) {
        return emitOpError("prop_names and prop_values must have the same count, but has ")
               << getPropNames().size() << " names and " << getPropValues().size() << " values";
    }

    return success();
}

LogicalResult CreateEdge::verify() {
    const StringAttr edgeType = dyn_cast<StringAttr>(getEdgeType());
    if (edgeType && edgeType.getValue().empty()) {
        return emitOpError("requires a non-empty edge type");
    }

    if (getPropNames().size() != getPropValues().size()) {
        return emitOpError("prop_names and prop_values must have the same count, but has ")
               << getPropNames().size() << " names and " << getPropValues().size() << " values";
    }

    return success();
}

LogicalResult Merge::verify() {
    const LogicalResult pattern = verifyMergePattern(getOperation(),
                                                     getNodeLabels(),
                                                     getNodePropNames(),
                                                     getEdgeTypes(),
                                                     getEdgePropNames(),
                                                     getEdgeDirectionsAttr(),
                                                     getPendingNodesAttr(),
                                                     getBoundNodes().size(),
                                                     getBoundPending().size(),
                                                     getNodePropValues().size(),
                                                     getEdgePropValues().size());
    if (failed(pattern)) {
        return pattern;
    }

    const size_t expectedResults = mergeResultCount(getNodeLabels(), getCarriedColumns().size());
    if (getResults().size() != expectedResults) {
        return emitOpError("must produce one column per chain entity, the created mask and one "
                           "per carried column - ")
               << expectedResults << " in all, but produces " << getResults().size();
    }

    return success();
}

LogicalResult SetNodeProperty::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    return success();
}

LogicalResult SetEdgeProperty::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    return success();
}

LogicalResult SetNodeProperties::verify() {
    if (!holdsIDsOf<storage::NodeIDType>(getInputNodes())) {
        return emitOpError("writes the properties of a column of node IDs");
    } else if (!writesAMapPerRow(getValue())) {
        return emitOpError("writes a column of maps or of tagged cells, optionally nullable, or the null literal");
    } else if (!::db::namesPropertyElementTypes(getCreatedTypesAttr())) {
        return emitOpError("creates each property with the element type of a stored value");
    }

    return success();
}

LogicalResult SetEdgeProperties::verify() {
    if (!holdsIDsOf<storage::EdgeIDType>(getInputEdges())) {
        return emitOpError("writes the properties of a column of edge IDs");
    } else if (!writesAMapPerRow(getValue())) {
        return emitOpError("writes a column of maps or of tagged cells, optionally nullable, or the null literal");
    } else if (!::db::namesPropertyElementTypes(getCreatedTypesAttr())) {
        return emitOpError("creates each property with the element type of a stored value");
    }

    return success();
}

LogicalResult ImportGraph::verify() {
    if (!::db::namesImportedPropertyTypes(getFormat(), getTypesAttr())) {
        return emitOpError("types each property as a datetime, duration or point a jsonl import reads, or a duration a parquet import reads");
    }

    return success();
}

// A label scan must name at least one label to filter by; a label-free scan of
// every node is db.scan_nodes, so an empty label list is malformed IR here.
LogicalResult ScanNodesByLabel::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

// The edge sibling of ScanNodesByLabel::verify: a label-free scan of every edge is
// db.scan_edges, so an empty label list is malformed IR here too.
LogicalResult ScanOutEdgesByLabelSrc::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult ScanInEdgesByLabelTgt::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult ScanOutEdgesByLabelTgt::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult ScanInEdgesByLabelSrc::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult ScanOutEdgesByLabelSrcTgt::verify() {
    if (getSrcLabels().empty() || getTgtLabels().empty()) {
        return emitOpError("requires at least one label on each end");
    }

    return success();
}

LogicalResult ScanInEdgesByLabelSrcTgt::verify() {
    if (getSrcLabels().empty() || getTgtLabels().empty()) {
        return emitOpError("requires at least one label on each end");
    }

    return success();
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

LogicalResult CountScanRows::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one scan to count");
    }

    const std::optional<llvm::StringRef> property = getProperty();
    if (property && property->empty()) {
        return emitOpError("requires a non-empty property name");
    }

    if (!property && getPropertyScan()) {
        return emitOpError("names the scan of a property it does not read");
    }

    if (getPropertyScan().value_or(0) >= getLabels().size()) {
        return emitOpError("reads a property from a scan it does not list");
    }

    return success();
}

LogicalResult ConstScanNodes::verify() {
    const Type node = columnElement(getResult());
    for (const Value bound : getBoundIDs()) {
        const Type element = columnElement(bound);
        if (!storage::isBoundEntityIDsType(element, node)) {
            return emitOpError("requires bound IDs of type i64, !storage.node_id or !storage.list, got ") << element;
        }
    }

    return success();
}

LogicalResult ConstScanEdges::verify() {
    const Type edge = columnElement(getEids());
    for (const Value bound : getBoundIDs()) {
        const Type element = columnElement(bound);
        if (!storage::isBoundEntityIDsType(element, edge)) {
            return emitOpError("requires bound IDs of type i64, !storage.edge_id or !storage.list, got ") << element;
        }
    }

    return success();
}

LogicalResult EntityIDOf::verify() {
    const Type element = columnElement(getInput());
    if (!storage::isEntityIDOfInputType(element)) {
        return emitOpError("requires a value, a node or an edge, not a path, got ") << element;
    }

    return success();
}

OpFoldResult EntityIDOf::fold(FoldAdaptor adaptor) {
    const Value input = getInput();
    const Type resultType = getType();

    if (input.getType() == resultType) {
        return input;
    }

    ElementID id = input.getDefiningOp<ElementID>();
    if (id && id.getInput().getType() == resultType) {
        return id.getInput();
    }

    return {};
}

LogicalResult ScanNodesByPropertyValue::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    const std::optional<TypedAttr> literal = getValue();
    const Value bound = getBoundValue();
    if (literal.has_value() == static_cast<bool>(bound)) {
        return emitOpError("requires either a literal or a bound value");
    }

    const Type valueType = literal ? literal->getType() : columnElement(bound);
    if (!storage::isPropertyScanType(valueType)) {
        return emitOpError(storage::propertyScanLiteralKinds()) << valueType;
    }

    const std::optional<ArrayAttr> labels = getLabels();
    if (labels && labels->empty()) {
        return emitOpError("requires at least one label when labels are given");
    }

    return success();
}

LogicalResult CheckLabelConstraint::verify() {
    const ArrayAttr alternatives = getAlternatives();
    const bool hasEmptyAlternative = llvm::any_of(alternatives, [](Attribute alternative) {
        return cast<ArrayAttr>(alternative).empty();
    });

    if (alternatives.empty() || hasEmptyAlternative) {
        return emitOpError("requires at least one label in every alternative");
    }

    return success();
}

ArrayAttr CheckLabelConstraint::getConjunction() {
    const ArrayAttr alternatives = getAlternatives();
    if (alternatives.size() != 1) {
        return nullptr;
    }

    return cast<ArrayAttr>(alternatives[0]);
}

LogicalResult CheckEdgeTypeConstraint::verify() {
    return verifyEdgeTypesNotEmpty(getOperation(), getEdgeTypes());
}

void Output::build(OpBuilder& builder, OperationState& state, ValueRange columns) {
    Output::build(builder, state, columns, ArrayAttr());
}

LogicalResult Output::verify() {
    const ArrayAttr columnNames = getColumnNamesAttr();
    if (columnNames && columnNames.size() != getColumns().size()) {
        return emitOpError("names must give one name per output column, but has ")
               << columnNames.size() << " names for " << getColumns().size() << " columns";
    }

    return success();
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

// db.limit passes its columns straight through, so the results must be exactly
// the input columns - same count, same types and in the same order.
LogicalResult Limit::verify() {
    if (failed(verifyPassThrough(getOperation(), getColumns(), getResults()))) {
        return failure();
    }

    return verifyCutCount(getOperation(), getCount(), getBoundCount());
}

// db.skip passes its columns straight through, the same as db.limit.
LogicalResult Skip::verify() {
    if (failed(verifyPassThrough(getOperation(), getColumns(), getResults()))) {
        return failure();
    }

    return verifyCutCount(getOperation(), getCount(), getBoundCount());
}

// db.sort passes its columns straight through reordered, so the results must be
// exactly the input columns - same count, same types, same order. The two key
// arrays must be parallel and every key must index a column that exists.
LogicalResult Sort::verify() {
    const OperandRange columns = getColumns();
    const Operation::result_range results = getResults();

    // A sort with no column has nothing to reorder, and its row count would have
    // no column to be read from during lowering. Reject it here at the db level.
    if (columns.empty()) {
        return emitOpError("requires at least one column");
    }

    if (columns.size() != results.size()) {
        return emitOpError("expects ") << columns.size()
                                       << " results, one per input column, but has "
                                       << results.size();
    }

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (columns[columnIndex].getType() != results[columnIndex].getType()) {
            return emitOpError("result ") << columnIndex
                                          << " must have the same type as input column "
                                          << columnIndex;
        }
    }

    const ArrayRef<int64_t> keyColumns = getKeyColumns();
    const ArrayRef<bool> keyAscending = getKeyAscending();

    // At least one key, and one direction per key: the two arrays describe the
    // same list of sort keys, so a mismatch is malformed IR.
    if (keyColumns.empty()) {
        return emitOpError("requires at least one sort key");
    }

    if (keyColumns.size() != keyAscending.size()) {
        return emitOpError("expects one ascending flag per key, but has ")
               << keyColumns.size() << " keys and " << keyAscending.size() << " flags";
    }

    // Every key names a column to sort by, so it must index one of the columns.
    for (const int64_t keyColumn : keyColumns) {
        const bool inRange = keyColumn >= 0 && static_cast<size_t>(keyColumn) < columns.size();
        if (!inRange) {
            return emitOpError("sort key ") << keyColumn << " is out of range for "
                                            << columns.size() << " columns";
        }
    }

    return success();
}

// db.remove_duplicates passes its columns straight through (minus duplicate rows),
// so the results must be exactly the input columns - same count, same types, same
// order - the shared pass-through check db.limit and db.skip use. The dedup key is
// the whole row, so - unlike db.sort - there are no key arrays to validate.
LogicalResult RemoveDuplicates::verify() {
    return verifyPassThrough(getOperation(), getColumns(), getResults());
}

// A call produces one column per yielded return value then one per carried column, so
// `yields`, the carry set and the results must line up. A call yielding nothing has no
// column to produce and no row count to be read from, so it is rejected here at the db
// level; the procedure's return values themselves are checked against the registry
// during lowering, which is where the registry is available.
LogicalResult CallProcedure::verify() {
    const ArrayAttr yields = getYields();
    const OperandRange carriedColumns = getCarriedColumns();

    // A call yielding nothing produces no column, so it has no row count for a carried
    // row to be replicated against - it is called for what it does, not for rows, and
    // nothing can ride through it.
    if (yields.empty() && !carriedColumns.empty()) {
        return emitOpError("yields no return value, so it cannot carry ")
               << carriedColumns.size() << " columns past it";
    }

    const Operation::result_range results = getResults();
    const size_t expectedResults = yields.size() + carriedColumns.size();
    if (results.size() != expectedResults) {
        return emitOpError("expects ") << expectedResults
                                       << " results, one per yielded return value and carried column, but has "
                                       << results.size();
    }

    // A carried column is only replicated by the call, never retyped, so each trailing
    // result keeps its carried column's type - the pass-through check db.limit shares,
    // applied to the results after the yields.
    for (size_t carriedIndex = 0; carriedIndex < carriedColumns.size(); carriedIndex++) {
        const Value carried = carriedColumns[carriedIndex];
        const Value result = results[yields.size() + carriedIndex];

        if (carried.getType() != result.getType()) {
            return emitOpError("carried result ") << carriedIndex
                                                  << " must have the same type as carried column "
                                                  << carriedIndex;
        }
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

// db.group_aggregate splits its columns into keyCount grouping keys followed by
// one input per aggregate, so keyCount and kinds must partition the columns with
// at least one of each. Its results are the key columns (passed through) then one
// per aggregate, so the key results must match their key columns; the aggregate
// result types are resolved during lowering and left unconstrained (as db.count).
LogicalResult GroupAggregate::verify() {
    const OperandRange columns = getColumns();
    const Operation::result_range results = getResults();
    const uint64_t keyCount = getKeyCount();
    const ArrayRef<int64_t> kinds = getKinds();

    // A grouped aggregate with no key is a whole-stream aggregate (db.count /
    // db.sum ...); with no aggregate it is a projection or DISTINCT. Either way it
    // is not this op, so require at least one of each.
    if (keyCount == 0) {
        return emitOpError("requires at least one grouping key");
    }

    if (kinds.empty()) {
        return emitOpError("requires at least one aggregate");
    }

    // Every column is either a grouping key or one aggregate's input, so the key and
    // aggregate counts must partition the columns exactly. keyCount is an unbounded
    // attribute, so bound it by the real column count first and compare the remainder
    // to kinds.size(): summing keyCount + kinds.size() would wrap in unsigned 64-bit
    // and let a pathological keyCount slip past the check into out-of-bounds lowering.
    const size_t columnCount = columns.size();
    if (keyCount > columnCount || columnCount - keyCount != kinds.size()) {
        return emitOpError("expects ") << keyCount
                                       << " grouping-key columns and " << kinds.size()
                                       << " aggregate columns, but has " << columnCount;
    }

    // One result per grouping key then one per aggregate.
    if (results.size() != columns.size()) {
        return emitOpError("expects ") << columns.size()
                                       << " results, one per grouping key and aggregate, but has "
                                       << results.size();
    }

    // The grouping keys pass through unchanged, so each key result keeps its key
    // column's type. The aggregate result types are resolved during lowering.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        if (columns[keyIndex].getType() != results[keyIndex].getType()) {
            return emitOpError("grouping-key result ") << keyIndex
                                                       << " must have the same type as key column "
                                                       << keyIndex;
        }
    }

    // Each kind names one aggregate's reduction, so it must be a valid
    // GroupAggregateKind (count / count_distinct / sum / sum_distinct / min / max /
    // avg / avg_distinct).
    for (const int64_t kind : kinds) {
        if (!storage::symbolizeGroupAggregateKind(kind)) {
            return emitOpError("has an unknown aggregate kind ") << kind;
        }
    }

    return success();
}

// db.collect splits its columns into keyCount grouping keys followed by exactly one
// collected value column, so operands are keyCount + 1. Its results are the key
// columns (passed through) then one list column holding the collected values.
LogicalResult Collect::verify() {
    const OperandRange columns = getColumns();
    const Operation::result_range results = getResults();
    const uint64_t keyCount = getKeyCount();

    // The collected columns follow the grouping keys, and one aggregate input follows
    // them per kind, so what is left over after the keys and the aggregates is the value
    // columns - at least one. Bound keyCount by the real column count first: summing
    // keyCount + aggregateCount could wrap in unsigned 64-bit and let a pathological
    // keyCount slip past into out-of-bounds lowering.
    const size_t columnCount = columns.size();
    const llvm::ArrayRef<int64_t> kinds = getKinds().value_or(llvm::ArrayRef<int64_t> {});
    const size_t aggregateCount = kinds.size();
    if (keyCount >= columnCount || columnCount - keyCount <= aggregateCount) {
        return emitOpError("expects ") << keyCount
                                       << " grouping-key columns, at least one collected column and "
                                       << aggregateCount
                                       << " aggregate columns, but has "
                                       << columnCount;
    }

    const size_t valueCount = columnCount - keyCount - aggregateCount;

    // One result per grouping key, one per collected list, then one per aggregate.
    if (results.size() != columnCount) {
        return emitOpError("expects ") << columnCount
                                       << " results, one per grouping key plus the collected lists and the "
                                          "aggregates, but has "
                                       << results.size();
    }

    // The grouping keys pass through unchanged, so each key result keeps its key
    // column's type.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        if (columns[keyIndex].getType() != results[keyIndex].getType()) {
            return emitOpError("grouping-key result ") << keyIndex
                                                       << " must have the same type as key column "
                                                       << keyIndex;
        }
    }

    // Each value's result is the list it collects into: a column whose element type is a
    // storage list of that value column's type.
    for (size_t valueIndex = 0; valueIndex < valueCount; valueIndex++) {
        const size_t position = keyCount + valueIndex;

        const ColumnType listColumn = llvm::dyn_cast<ColumnType>(results[position].getType());
        if (!listColumn || !llvm::isa<storage::ListType>(listColumn.getType())) {
            return emitOpError("collected result ") << valueIndex << " must be a list column";
        }

        const ColumnType valueColumn = llvm::cast<ColumnType>(columns[position].getType());
        const storage::ListType collectedList = llvm::cast<storage::ListType>(listColumn.getType());
        if (collectedList.getElementType() != valueColumn.getType()) {
            return emitOpError("collected result ") << valueIndex
                                                    << " must be a list of "
                                                    << valueColumn.getType()
                                                    << ", the collected column's type, but collects "
                                                    << collectedList.getElementType();
        }
    }

    // Each kind names one aggregate's reduction, so it must be a valid
    // GroupAggregateKind, exactly as db.group_aggregate's kinds must.
    for (const int64_t kind : kinds) {
        if (!storage::symbolizeGroupAggregateKind(kind)) {
            return emitOpError("has an unknown aggregate kind ") << kind;
        }
    }

    // Every deduplicating value names one of the collected columns.
    if (const std::optional<llvm::ArrayRef<int64_t>> distinctValues = getDistinctValues()) {
        for (const int64_t valueIndex : *distinctValues) {
            if (valueIndex < 0 || static_cast<size_t>(valueIndex) >= valueCount) {
                return emitOpError("distinct value index ") << valueIndex
                                                            << " is out of range for "
                                                            << valueCount
                                                            << " collected columns";
            }
        }
    }

    return success();
}

// db.unwind_collect has the same column shape as db.collect - keyCount grouping keys
// then one collected value column - but re-emits one scalar row per element, so its
// results are the key columns (passed through) then one value column.
LogicalResult UnwindCollect::verify() {
    const OperandRange columns = getColumns();
    const Operation::result_range results = getResults();
    const uint64_t keyCount = getKeyCount();

    const size_t columnCount = columns.size();
    if (keyCount >= columnCount || columnCount - keyCount != 1) {
        return emitOpError("expects ") << keyCount
                                       << " grouping-key columns and one collected column, but has "
                                       << columnCount;
    }

    if (results.size() != columnCount) {
        return emitOpError("expects ") << columnCount
                                       << " results, one per grouping key plus the unwound value, but has "
                                       << results.size();
    }

    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        if (columns[keyIndex].getType() != results[keyIndex].getType()) {
            return emitOpError("grouping-key result ") << keyIndex
                                                       << " must have the same type as key column "
                                                       << keyIndex;
        }
    }

    if (columns[keyCount].getType() != results[keyCount].getType()) {
        return emitOpError("unwound value result must have the same type as the collected column");
    }

    return success();
}

LogicalResult Unwind::verify() {
    return verifyCarriedResults(getOperation(), getColumnsToFilter(), getCarried());
}

// The source block takes one argument per carried column, of its type, and ends naming
// the source.
LogicalResult UnwindComputed::verify() {
    Block& sourceBlock = getSource().front();

    if (sourceBlock.empty() || !isa<UnwindYield>(sourceBlock.back())) {
        return emitOpError("source region must end with a db.unwind_yield");
    }

    const OperandRange carried = getColumnsToFilter();
    if (sourceBlock.getNumArguments() != carried.size()) {
        return emitOpError("source region takes one argument per carried column, ")
               << "expected " << carried.size() << " but has " << sourceBlock.getNumArguments();
    }

    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        const Type argumentType = sourceBlock.getArgument(static_cast<unsigned>(carriedIndex)).getType();

        if (argumentType != carried[carriedIndex].getType()) {
            return emitOpError("source argument ") << carriedIndex
                                                   << " must have the type of carried column "
                                                   << carriedIndex;
        }
    }

    return verifyCarriedResults(getOperation(), carried, getCarried());
}

LogicalResult UnwindRange::verify() {
    const ColumnType elementColumn = dyn_cast<ColumnType>(getElement().getType());
    const Type element = elementColumn ? elementColumn.getType() : Type {};
    const storage::NullableType nullable = dyn_cast_or_null<storage::NullableType>(element);

    const bool nullableInteger = nullable && isa<IntegerType>(nullable.getValueType());
    const bool resolvedInLowering = isa_and_nonnull<NoneType>(element);
    if (!nullableInteger && !resolvedInLowering) {
        return emitOpError("element must be a column of nullable integers, or of none for lowering to resolve");
    }

    return verifyCarriedResults(getOperation(), getColumnsToFilter(), getCarried());
}

// The body binds the element, the row tag and one argument per carried column, and ends
// naming what each element contributes.
LogicalResult ListComprehension::verify() {
    return verifyElementBody(getOperation(), getBody().front(), getColumnsToFilter());
}

LogicalResult ListPredicate::verify() {
    return verifyElementBody(getOperation(), getBody().front(), getColumnsToFilter());
}

LogicalResult Reduce::verify() {
    return verifyFoldRegion<ReduceYield>(getOperation(), getBody().front(), getColumnsToFilter(), "db.reduce_yield");
}

LogicalResult AllReduce::verify() {
    const OperandRange carried = getColumnsToFilter();

    if (failed(verifyFoldRegion<ReduceYield>(getOperation(), getBody().front(), carried, "db.reduce_yield"))) {
        return failure();
    }

    return verifyFoldRegion<AllReduceYield>(getOperation(), getPredicate().front(), carried, "db.all_reduce_yield");
}

// The pattern takes one argument per input column and the row tag, and ends naming what
// each match contributes to the list of the row it came from.
LogicalResult PatternComprehension::verify() {
    Block& patternBlock = getPattern().front();

    auto yield = dyn_cast_or_null<ComprehensionYield>(patternBlock.empty() ? nullptr : &patternBlock.back());
    if (!yield) {
        return emitOpError("pattern region must end with a db.comprehension_yield");
    }

    const mlir::OperandRange inputs = getInputColumns();
    const size_t inputCount = inputs.size();

    // No input column means no input row to tag: the one list covers the single empty row
    // the query starts from, which every match belongs to.
    const size_t expectedArguments = inputCount == 0 ? 0 : inputCount + 1;

    if (patternBlock.getNumArguments() != expectedArguments) {
        return emitOpError("pattern region takes one argument per input column plus the row tag, ")
               << "expected " << expectedArguments << " but has " << patternBlock.getNumArguments();
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        if (patternBlock.getArgument(inputIndex).getType() != inputs[inputIndex].getType()) {
            return emitOpError("pattern argument ") << inputIndex << " must have the type of input column "
                                                    << inputIndex;
        }
    }

    if (inputCount != 0) {
        const auto rowTagType =
            llvm::dyn_cast<ColumnType>(patternBlock.getArgument(inputCount).getType());

        if (!rowTagType || !rowTagType.getType().isUnsignedInteger(64)) {
            return emitOpError("pattern argument ") << inputCount << " must be the ui64 column of row tags";
        }
    }

    const bool tagsRows = inputCount != 0;
    const bool yieldsATag = yield.getRowTags() != nullptr;

    if (tagsRows != yieldsATag) {
        return emitOpError("the pattern yields a row tag exactly when the op takes input columns");
    }

    return success();
}

// There is a column for the list to be built out of, and what the op produces is a list.
LogicalResult MakeList::verify() {
    if (getElements().empty()) {
        return emitOpError("requires at least one element column");
    }

    const ColumnType resultColumn = llvm::dyn_cast<ColumnType>(getResult().getType());
    if (!resultColumn) {
        return emitOpError("result must be a column");
    }

    if (!llvm::isa<storage::ListType>(resultColumn.getType())) {
        return emitOpError("result must be a column of lists");
    }

    return success();
}

LogicalResult MakeMap::verify() {
    const size_t valueCount = getValues().size();
    if (valueCount == 0) {
        return emitOpError("requires at least one value column");
    }

    const ArrayAttr keys = getKeys();
    if (keys.size() != valueCount) {
        return emitOpError("requires one key per value column");
    }

    llvm::SmallDenseSet<StringRef, 8> seenKeys;
    for (const Attribute key : keys) {
        if (!seenKeys.insert(llvm::cast<StringAttr>(key).getValue()).second) {
            return emitOpError("requires distinct keys");
        }
    }

    return success();
}

// A tagged cell may hold a map, and a column whose type lowering resolves may be one
LogicalResult MergeMaps::verify() {
    const OperandRange maps = getMaps();
    if (maps.size() < 2) {
        return emitOpError("requires at least two map columns");
    }

    const auto readsMaps = [](Value column) {
        return isa<storage::MapType, storage::AnyType, NoneType>(columnElement(column));
    };

    if (!llvm::all_of(maps, readsMaps)) {
        return emitOpError("operands must be columns of maps");
    }

    return success();
}

// The integers a range spans ride the list it builds, so the result is a column of them.
LogicalResult Range::verify() {
    const ColumnType resultColumn = llvm::dyn_cast<ColumnType>(getResult().getType());
    if (!resultColumn) {
        return emitOpError("result must be a column");
    }

    const storage::ListType listType = llvm::dyn_cast<storage::ListType>(resultColumn.getType());
    if (!listType) {
        return emitOpError("result must be a column of lists");
    }

    if (!llvm::isa<mlir::IntegerType>(listType.getElementType())) {
        return emitOpError("result must be a column of integer lists");
    }

    return success();
}

LogicalResult SortMulti::verify() {
    const Value fields = getFields();
    const Value limit = getLimit();
    const Value skip = getSkip();

    const bool readsLists = isColumnOf<storage::ListType>(getList().getType())
                         && (!fields || isColumnOf<storage::ListType>(fields.getType()));
    const bool readsIntegerBounds = (!limit || isIntegerColumn(limit)) && (!skip || isIntegerColumn(skip));

    if (!readsLists) {
        return emitOpError("sorts a column of lists on a column of lists of fields");
    } else if (!readsIntegerBounds) {
        return emitOpError("limit and skip must be integer columns");
    }

    return success();
}

LogicalResult Replace::verify() {
    const bool readsStrings = llvm::all_of(getOperation()->getOperands(), [](Value operand) {
        return isColumnOf<storage::StringType>(operand.getType());
    });

    if (!readsStrings) {
        return emitOpError("operands must be string columns");
    }

    return verifyStringResult(getOperation(), getResult());
}

LogicalResult PointDistance::verify() {
    return verifyPointFunction<Float64Type>(getOperation(), getResult(), "f64");
}

LogicalResult PointWithinBBox::verify() {
    return verifyPointFunction<storage::BoolType>(getOperation(), getResult(), "booleans");
}

LogicalResult Split::verify() {
    const Value delimiter = getDelimiter();

    const bool readsAString = isColumnOf<storage::StringType>(getString().getType());
    const bool readsDelimiters = isColumnOf<storage::StringType>(delimiter.getType()) || isa<storage::ListType>(columnElement(delimiter));
    if (!readsAString || !readsDelimiters) {
        return emitOpError("operands must be a string column, then a string or list column");
    }

    const ColumnType resultColumn = llvm::dyn_cast<ColumnType>(getResult().getType());
    const storage::ListType listType = resultColumn ? llvm::dyn_cast<storage::ListType>(resultColumn.getType())
                                                    : storage::ListType {};

    if (!listType || !llvm::isa<storage::StringType>(listType.getElementType())) {
        return emitOpError("result must be a column of string lists");
    }

    return success();
}

// The unwound column's element type is the homogeneity verdict: a type-erased
// A !storage.any column accepts any elements, a typed one requires them to share that one
// type - the shared check the const_list verifier runs too.
LogicalResult UnwindConst::verify() {
    const ColumnType resultColumn = llvm::dyn_cast<ColumnType>(getResult().getType());
    if (!resultColumn) {
        return emitOpError("result must be a column");
    }

    const mlir::Type elementType = resultColumn.getType();
    if (llvm::isa<storage::AnyType>(elementType)) {
        return success();
    }

    return verifyHomogeneousElements(getOperation(), getElements());
}

// One column per field, and a field is named either by position or by header - a
// header only where a header line was read, since nothing else resolves the name.
LogicalResult LoadCSV::verify() {
    const Value boundPath = getBoundPath();
    if (getPath().has_value() == static_cast<bool>(boundPath)) {
        return emitOpError("requires either a path or a bound path");
    }

    const Type boundType = boundPath ? cast<ColumnType>(boundPath.getType()).getType() : Type();
    if (boundType && !isa<storage::StringType>(boundType)) {
        return emitOpError("requires a bound path of type !storage.string, got ") << boundType;
    }

    const ArrayAttr fields = getFieldsAttr();

    // A load producing no column produces no rows either: nothing downstream could read
    // the records it opened, and its loop would have no chunk to size a step from
    if (fields.empty()) {
        return emitOpError("requires at least one field");
    }

    if (fields.size() != getResults().size()) {
        return emitOpError("produces one column per field, but names ")
               << fields.size() << " fields for " << getResults().size() << " columns";
    }

    for (const Attribute field : fields) {
        if (const auto header = llvm::dyn_cast<StringAttr>(field)) {
            if (!getWithHeaders()) {
                return emitOpError("names field '") << header.getValue()
                                                    << "' by header, which only a with_headers load resolves";
            }
        } else {
            const auto index = llvm::dyn_cast<IntegerAttr>(field);
            if (!index || !index.getType().isUnsignedInteger(64)) {
                return emitOpError("field must be a ui64 position or a header name");
            }
        }
    }

    return success();
}

LogicalResult CreateVectorIndex::verify() {
    if (getDimension() == 0) {
        return emitOpError("must hold vectors of at least one dimension");
    }

    return success();
}

// A search reporting no neighbour, or searching for no vector, is a query that asked for
// nothing: neither is a shape the index can be asked for, so both are rejected here rather
// than left to return an empty result the query would read as "no match".
LogicalResult VectorSearch::verify() {
    const Value boundIndexName = getBoundIndexName();
    if (getIndexName().has_value() == static_cast<bool>(boundIndexName)) {
        return emitOpError("requires either an index name or a bound one");
    }

    const std::optional<uint64_t> neighbourCount = getK();
    const Value boundCount = getBoundK();
    if (neighbourCount.has_value() == static_cast<bool>(boundCount)) {
        return emitOpError("requires either a neighbour count or a bound one");
    } else if (neighbourCount == 0) {
        return emitOpError("must report at least one neighbour");
    }

    const std::optional<llvm::ArrayRef<float>> queryVector = getQueryVector();
    const Value boundQueryVector = getBoundQueryVector();
    if (queryVector.has_value() == static_cast<bool>(boundQueryVector)) {
        return emitOpError("requires either a query vector or a bound one");
    } else if (queryVector && queryVector->empty()) {
        return emitOpError("must search for a vector of at least one dimension");
    }

    Operation* const op = getOperation();
    MLIRContext* const context = getContext();

    const bool boundValuesVerify = succeeded(verifySearchedValue(op, "index name", boundIndexName, storage::StringType::get(context)))
                                && succeeded(verifySearchedValue(op, "neighbour count", boundCount, IntegerType::get(context, 64)))
                                && succeeded(verifySearchedValue(op, "query vector", boundQueryVector, storage::EmbeddingType::get(context)));

    return success(boundValuesVerify);
}

// The region's entry block stands for the rows in flight: one argument per input
// column, then the row tag. The db.optional_yield names the tag and the columns
// the region contributes - the input columns as the pattern left them, then the
// pattern's own variables - and the op's results are exactly those columns. A
// pattern variable is what a row the pattern missed comes back with null, so it
// has to be an ID column: those carry an invalid ID, which is how a null entity
// is spelled, where a property value column would need a null of its own.
LogicalResult OptionalMatch::verify() {
    Block& patternBlock = getPattern().front();

    auto yield = dyn_cast_or_null<OptionalYield>(patternBlock.empty() ? nullptr : &patternBlock.back());
    if (!yield) {
        return emitOpError("pattern region must end with a db.optional_yield");
    }

    const size_t inputCount = getInputColumns().size();

    // No input column means no input row to tag: the rows the pattern joins onto are
    // the single empty row the query starts from.
    const size_t expectedArguments = inputCount == 0 ? 0 : inputCount + 1;

    if (patternBlock.getNumArguments() != expectedArguments) {
        return emitOpError("pattern region takes one argument per input column plus the row tag, ")
               << "expected " << expectedArguments << " but has " << patternBlock.getNumArguments();
    }

    const bool tagsRows = inputCount != 0;
    const bool yieldsATag = yield.getTag() != nullptr;

    if (tagsRows != yieldsATag) {
        return emitOpError("the pattern yields a row tag exactly when the op takes input columns");
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        if (patternBlock.getArgument(inputIndex).getType() != getInputColumns()[inputIndex].getType()) {
            return emitOpError("pattern argument ") << inputIndex << " must have the type of input column "
                                                    << inputIndex;
        }
    }

    const ValueRange yieldedColumns = yield.getColumns();
    if (yieldedColumns.empty()) {
        return emitOpError("pattern must yield at least one column");
    }

    if (yieldedColumns.size() < inputCount) {
        return emitOpError("pattern must yield at least one column per input column, expected ")
               << inputCount << " but has " << yieldedColumns.size();
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        const mlir::Type inputType = getInputColumns()[inputIndex].getType();

        // A traversal rebinds its source to a typed ID column, so a pattern walking from a
        // column whose element type is still unspecified - what a CALL yields - hands it
        // back refined. Any other change of type is a carry set that lost track of a column.
        const auto inputColumn = llvm::dyn_cast<ColumnType>(inputType);
        const bool refinesAnUnspecifiedColumn = inputColumn
                                                && llvm::isa<mlir::NoneType>(inputColumn.getType());

        if (yieldedColumns[inputIndex].getType() != inputType && !refinesAnUnspecifiedColumn) {
            return emitOpError("yielded column ") << inputIndex << " must have the type of input column "
                                                  << inputIndex;
        }
    }

    for (size_t columnIndex = inputCount; columnIndex < yieldedColumns.size(); columnIndex++) {
        const auto column = llvm::dyn_cast<ColumnType>(yieldedColumns[columnIndex].getType());
        const mlir::Type elementType = column ? column.getType() : mlir::Type();

        // A function the pattern computes is typed during lowering, which pads the value it
        // resolves to
        const bool isPaddable = llvm::isa_and_present<storage::NodeIDType,
                                                      storage::EdgeIDType,
                                                      storage::ListType,
                                                      storage::PathType,
                                                      storage::NullableType,
                                                      mlir::NoneType>(elementType);

        if (!isPaddable) {
            return emitOpError("pattern variable column ")
                   << columnIndex << " must be a node or edge ID, list, path, nullable or function result column, "
                   << "so a missed match can be null";
        }
    }

    const Operation::result_type_range resultTypes = getOperation()->getResultTypes();
    if (resultTypes.size() != yieldedColumns.size()) {
        return emitOpError("expects ") << yieldedColumns.size()
                                       << " results, the columns the pattern yields, but has "
                                       << resultTypes.size();
    }

    for (size_t resultIndex = 0; resultIndex < resultTypes.size(); resultIndex++) {
        if (resultTypes[resultIndex] != yieldedColumns[resultIndex].getType()) {
            return emitOpError("result ") << resultIndex << " must be the yielded column type "
                                          << yieldedColumns[resultIndex].getType();
        }
    }

    return success();
}

LogicalResult ExistsSubquery::verify() {
    return verifySubqueryExpression<ExistsYield>(*this);
}

LogicalResult CountSubquery::verify() {
    return verifySubqueryExpression<CountSubqueryYield>(*this);
}

LogicalResult CollectSubquery::verify() {
    return verifySubqueryExpression<CollectSubqueryYield>(*this);
}

LogicalResult RowConstant::verify() {
    const auto argument = dyn_cast<BlockArgument>(getRow());
    Operation* const body = argument ? argument.getOwner()->getParentOp() : nullptr;

    if (!body || !runsItsBodyPerRow(body)) {
        return emitOpError("operand must be an argument of a body run once per row");
    }

    return success();
}

LogicalResult CallSubquery::verify() {
    Block& bodyBlock = getBody().front();

    auto yield = dyn_cast_or_null<SubqueryYield>(bodyBlock.empty() ? nullptr : &bodyBlock.back());
    if (!yield) {
        return emitOpError("body region must end with a db.subquery_yield");
    }

    const OperandRange inputs = getInputColumns();
    const size_t inputCount = inputs.size();

    // The body carries the row tag through its dataflow only when it carries the scope,
    // and only over rows it has: a body run one row at a time, or over the single empty
    // row, is tagged by the lowering instead.
    const bool tagsRows = getOptional() && getCarriesScope() && inputCount > 0;

    const size_t expectedArguments = tagsRows ? inputCount + 1 : inputCount;
    if (bodyBlock.getNumArguments() != expectedArguments) {
        return emitOpError("body region takes one argument per input column")
               << (tagsRows ? " plus the row tag" : "") << ", expected " << expectedArguments
               << " but has " << bodyBlock.getNumArguments();
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        if (bodyBlock.getArgument(inputIndex).getType() != inputs[inputIndex].getType()) {
            return emitOpError("body argument ") << inputIndex << " must have the type of input column "
                                                 << inputIndex;
        }
    }

    const bool yieldsATag = yield.getTag() != nullptr;
    if (tagsRows != yieldsATag) {
        return emitOpError("the body yields a row tag exactly when it takes one");
    }

    const bool readsArgumentsAsColumns = !getCarriesScope()
                                      && failed(verifyArgumentsReadAsRunConstants(getOperation(), bodyBlock));
    if (readsArgumentsAsColumns) {
        return failure();
    }

    const ValueRange yieldedColumns = yield.getColumns();
    const Operation::result_type_range resultTypes = getOperation()->getResultTypes();

    if (getUnit()) {
        if (getOptional()) {
            return emitOpError("a unit subquery yields no row for OPTIONAL to pad");
        }

        if (!yieldedColumns.empty()) {
            return emitOpError("a unit body yields no column");
        }

        if (resultTypes.size() != 0) {
            return emitOpError("a unit subquery has no result");
        }

        return success();
    }

    // The columns the results stand for: what the body yields when it carries the scope
    // itself, and the inputs ahead of what it yields otherwise.
    llvm::SmallVector<Type, 8> expectedResults;
    if (getCarriesScope()) {
        if (yieldedColumns.size() < inputCount) {
            return emitOpError("a body carrying its scope yields at least one column per input column, expected ")
                   << inputCount << " but has " << yieldedColumns.size();
        }

        for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
            const Type inputType = inputs[inputIndex].getType();

            // A traversal rebinds its source to a typed ID column, so a body walking from a
            // column whose element type is still unspecified hands it back refined.
            const auto inputColumn = llvm::dyn_cast<ColumnType>(inputType);
            const bool refinesAnUnspecifiedColumn = inputColumn && llvm::isa<mlir::NoneType>(inputColumn.getType());

            if (yieldedColumns[inputIndex].getType() != inputType && !refinesAnUnspecifiedColumn) {
                return emitOpError("yielded column ") << inputIndex << " must have the type of input column "
                                                      << inputIndex;
            }
        }
    } else {
        for (const Value input : inputs) {
            expectedResults.push_back(input.getType());
        }
    }

    for (const Value yielded : yieldedColumns) {
        expectedResults.push_back(yielded.getType());
    }

    if (resultTypes.size() != expectedResults.size()) {
        return emitOpError("expects ") << expectedResults.size()
                                       << " results, one per input column then one per yielded column, but has "
                                       << resultTypes.size();
    }

    for (size_t resultIndex = 0; resultIndex < expectedResults.size(); resultIndex++) {
        if (resultTypes[resultIndex] != expectedResults[resultIndex]) {
            return emitOpError("result ") << resultIndex << " must have type " << expectedResults[resultIndex];
        }
    }

    return success();
}

Speculation::Speculatability CheckedCast::getSpeculatability() {
    return speculatableOverKnownTypes(getOperation()->getOperands());
}

bool FilterOp::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._operandOffset = 1, ._count = getColumnsToFilter().size()};
    return true;
}

bool Limit::getCarrySet(CarrySet& carrySet) {
    const MutableOperandRange carried = getCarrySetMutable();
    carrySet = CarrySet {._operandOffset = operandRunOffset(carried), ._count = carried.size()};
    return true;
}

MutableOperandRange Limit::getCarrySetMutable() {
    return getColumnsMutable();
}

bool Skip::getCarrySet(CarrySet& carrySet) {
    const MutableOperandRange carried = getCarrySetMutable();
    carrySet = CarrySet {._operandOffset = operandRunOffset(carried), ._count = carried.size()};
    return true;
}

MutableOperandRange Skip::getCarrySetMutable() {
    return getColumnsMutable();
}

bool Sort::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._count = getColumns().size()};
    return true;
}

bool RemoveDuplicates::getCarrySet(CarrySet& carrySet) {
    const MutableOperandRange carried = getCarrySetMutable();
    carrySet = CarrySet {._operandOffset = operandRunOffset(carried), ._count = carried.size()};
    return true;
}

MutableOperandRange RemoveDuplicates::getCarrySetMutable() {
    return getColumnsMutable();
}

bool GroupAggregate::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._count = getKeyCount()};
    return true;
}

bool Collect::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._count = getKeyCount()};
    return true;
}

bool FetchNodes::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._operandOffset = 1, ._resultOffset = 1, ._count = getColumnsToFilter().size()};
    return true;
}

bool FetchEdges::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._operandOffset = 1, ._resultOffset = 4, ._count = getColumnsToFilter().size()};
    return true;
}

bool Unwind::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._operandOffset = 1, ._resultOffset = 1, ._count = getColumnsToFilter().size()};
    return true;
}

bool UnwindRange::getCarrySet(CarrySet& carrySet) {
    const MutableOperandRange carried = getCarrySetMutable();
    carrySet = CarrySet {._operandOffset = operandRunOffset(carried), ._resultOffset = 1, ._count = carried.size()};
    return true;
}

MutableOperandRange UnwindRange::getCarrySetMutable() {
    return getColumnsToFilterMutable();
}

bool UnwindComputed::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._resultOffset = 1, ._count = getColumnsToFilter().size()};
    return true;
}

void UnwindComputed::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    collectRowSource(value, carried);
    collectCarriedArgument(value, getColumnsToFilter(), carried);
}

bool CallProcedure::getCarrySet(CarrySet& carrySet) {
    const MutableOperandRange carried = getCarrySetMutable();
    carrySet = CarrySet {._operandOffset = operandRunOffset(carried),
                         ._resultOffset = getYields().size(),
                         ._count = carried.size()};
    return true;
}

MutableOperandRange CallProcedure::getCarrySetMutable() {
    return getCarriedColumnsMutable();
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

bool ExplorePaths::getCarrySet(CarrySet& carrySet) {
    const MutableOperandRange carried = getCarrySetMutable();
    carrySet = CarrySet {._operandOffset = operandRunOffset(carried), ._resultOffset = PATH_FIXED_RESULT_COUNT, ._count = carried.size()};
    return true;
}

MutableOperandRange ExplorePaths::getCarrySetMutable() {
    return getColumnsToFilterMutable();
}

bool ExplorePaths::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    const MutableOperandRange carried = getRegionCarrySetMutable();
    carrySet = RegionCarrySet {._operandOffset = operandRunOffset(carried),
                               ._argumentOffset = hopWalkArgumentCount(region.getRegionNumber()),
                               ._count = carried.size()};
    return true;
}

MutableOperandRange ExplorePaths::getRegionCarrySetMutable() {
    return getHopImportsMutable();
}

// The seed comes back as srcids, and as tgtids too when the walk ends on it
Value ExplorePaths::getRowSource(OpResult result) {
    const bool holdsTheSeed = result == getSrcids() || (result == getTgtids() && getEndsOnSeed());
    if (holdsTheSeed) {
        return getInputNodes();
    }

    return carrySetRowSource(getOperation(), result);
}

Value CrossProduct::getRowSource(OpResult result) {
    return yieldedFactorColumn(getLeftFactor(), getRightFactor(), result.getResultNumber());
}

bool CrossProduct::getYieldedResultOffset(Region& region, size_t& resultOffset) {
    resultOffset = factorResultOffset(getLeftFactor(), region);
    return true;
}

Value HashJoin::getRowSource(OpResult result) {
    return yieldedFactorColumn(getLeftFactor(), getRightFactor(), result.getResultNumber());
}

bool HashJoin::getYieldedResultOffset(Region& region, size_t& resultOffset) {
    resultOffset = factorResultOffset(getLeftFactor(), region);
    return true;
}

bool ListComprehension::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    fillTrailingRegionCarrySet(getOperation(), region, getColumnsToFilter().size(), carrySet);
    return true;
}

bool ListPredicate::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    fillTrailingRegionCarrySet(getOperation(), region, getColumnsToFilter().size(), carrySet);
    return true;
}

bool Reduce::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    fillTrailingRegionCarrySet(getOperation(), region, getColumnsToFilter().size(), carrySet);
    return true;
}

bool AllReduce::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    fillTrailingRegionCarrySet(getOperation(), region, getColumnsToFilter().size(), carrySet);
    return true;
}

bool PatternComprehension::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    carrySet = RegionCarrySet {._count = getInputColumns().size()};
    return true;
}

bool OptionalMatch::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    carrySet = RegionCarrySet {._count = getInputColumns().size(), ._handsBack = true, ._yieldsBack = true};
    return true;
}

void OptionalMatch::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    OptionalYield yield = cast<OptionalYield>(getPattern().front().getTerminator());

    collectCarriedResult(value, 0, yield.getColumns(), carried);
    collectCarriedArgument(value, getInputColumns(), carried);
}

// A body carrying its scope yields the inputs back ahead of its own columns; otherwise the
// inputs come back unchanged ahead of what the body yields
void CallSubquery::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    SubqueryYield yield = cast<SubqueryYield>(getBody().front().getTerminator());
    const OperandRange inputs = getInputColumns();

    if (getCarriesScope()) {
        collectCarriedResult(value, 0, yield.getColumns(), carried);
    } else {
        collectCarriedResult(value, 0, inputs, carried);
        collectCarriedResult(value, inputs.size(), yield.getColumns(), carried);
    }

    collectCarriedArgument(value, inputs, carried);
}

bool CallSubquery::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    const bool handsBack = !getUnit();
    carrySet = RegionCarrySet {._count = getInputColumns().size(),
                               ._handsBack = handsBack,
                               ._yieldsBack = handsBack && getCarriesScope()};
    return true;
}

// A row the body missed comes out of an optional call with what it yields null
bool CallSubquery::getYieldedResultOffset(Region& region, size_t& resultOffset) {
    if (getUnit() || getOptional()) {
        return false;
    }

    resultOffset = getCarriesScope() ? 0 : getInputColumns().size();
    return true;
}

void ExistsSubquery::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    collectCarriedArgument(value, getInputColumns(), carried);
}

bool ExistsSubquery::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    carrySet = RegionCarrySet {._count = getInputColumns().size()};
    return true;
}

void CountSubquery::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    collectCarriedArgument(value, getInputColumns(), carried);
}

bool CountSubquery::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    carrySet = RegionCarrySet {._count = getInputColumns().size()};
    return true;
}

void CollectSubquery::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    collectCarriedArgument(value, getInputColumns(), carried);
}

bool CollectSubquery::getRegionCarrySet(Region& region, RegionCarrySet& carrySet) {
    carrySet = RegionCarrySet {._count = getInputColumns().size()};
    return true;
}

void Union::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    collectBranchYields(value, getBranches(), carried);
}

void ScanMaterialized::collectCarriedColumns(Value value, llvm::SmallVectorImpl<Value>& carried) {
    Materialize materialize = getRows().getDefiningOp<Materialize>();
    if (materialize) {
        collectCarriedResult(value, 0, materialize.getColumns(), carried);
    }
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

Speculation::Speculatability Range::getSpeculatability() {
    int64_t from = 0;
    int64_t to = 0;
    int64_t by = 1;

    const Value step = getStep();
    const bool literalBounds = matchIntegerLiteral(getStart(), from) && matchIntegerLiteral(getEnd(), to);
    const bool literalStep = !step || matchIntegerLiteral(step, by);
    const bool knownList = literalBounds && literalStep && by != 0;

    if (!knownList) {
        return Speculation::NotSpeculatable;
    }

    return speculatableWhen(::db::countRangeSteps(from, to, by) < ::db::RANGE_LENGTH_LIMIT);
}

Speculation::Speculatability ListSlice::getSpeculatability() {
    const Value from = getFrom();
    const Value to = getTo();
    const bool readsIntegerBounds = (!from || holdsOnlyIntegers(from)) && (!to || holdsOnlyIntegers(to));

    return speculatableWhen(holdsOnlyLists(getList()) && readsIntegerBounds);
}

// A key is sorted on in the order <=> gives cells, which raises on an embedding, and a list
// or a map may hold one at any depth. With no field the list is kept as it is.
Speculation::Speculatability SortMulti::getSpeculatability() {
    const Value list = getList();
    const Value fields = getFields();
    const Value limit = getLimit();
    const Value skip = getSkip();

    int64_t skipped = 0;
    const bool skipsALiteralCount = !skip || (matchIntegerLiteral(skip, skipped) && skipped >= 0);
    const bool readsIntegerLimit = !limit || holdsOnlyIntegers(limit);

    ConstantOp fieldsLiteral = fields ? fields.getDefiningOp<ConstantOp>() : ConstantOp {};
    const ArrayAttr names = fieldsLiteral ? dyn_cast<ArrayAttr>(fieldsLiteral.getValue()) : ArrayAttr {};

    if (!skipsALiteralCount || !readsIntegerLimit || !holdsOnlyLists(list)) {
        return Speculation::NotSpeculatable;
    } else if (!fields || (names && names.empty())) {
        return Speculation::Speculatable;
    } else if (!names || !::db::readsHoldOnlyNullOr(list, {StringAttr {}}, {::db::CellTag::MapView})) {
        return Speculation::NotSpeculatable;
    }

    for (const Attribute name : names) {
        const StringAttr field = dyn_cast<StringAttr>(name);
        if (!field) {
            return Speculation::NotSpeculatable;
        }

        const StringRef text = field.getValue();
        const StringAttr key = StringAttr::get(getContext(), text.starts_with("^") ? text.drop_front() : text);
        const StringAttr keyOfEachElement[] = {StringAttr {}, key};

        const bool keyHasAnOrder = ::db::readsHoldOnlyNullOr(list,
                                                             keyOfEachElement,
                                                             {::db::CellTag::Int,
                                                              ::db::CellTag::UInt,
                                                              ::db::CellTag::Double,
                                                              ::db::CellTag::String,
                                                              ::db::CellTag::Bool,
                                                              ::db::CellTag::DateTime,
                                                              ::db::CellTag::Duration,
                                                              ::db::CellTag::Point,
                                                              ::db::CellTag::NodeID,
                                                              ::db::CellTag::EdgeID,
                                                              ::db::CellTag::Path});

        if (!keyHasAnOrder) {
            return Speculation::NotSpeculatable;
        }
    }

    return Speculation::Speculatable;
}

Speculation::Speculatability ListIndex::getSpeculatability() {
    return speculatableWhen(holdsOnlyLists(getLhs()));
}

Speculation::Speculatability StaticMapKey::getSpeculatability() {
    return speculatableWhen(holdsOnlyKeyedValues(getMap()));
}

Speculation::Speculatability DynamicMapKey::getSpeculatability() {
    return speculatableWhen(holdsOnlyKeyedValues(getMap()));
}

// The window raises as the slice it was folded from does, on a type-erased bound holding no
// integer
Speculation::Speculatability UnwindRange::getSpeculatability() {
    int64_t by = 0;
    const bool countsByALiteralStep = matchIntegerLiteral(getStep(), by) && by != 0;
    if (!countsByALiteralStep) {
        return Speculation::NotSpeculatable;
    }

    llvm::SmallVector<Value, 4> bounds {getStart(), getEnd()};
    if (const Value from = getFrom()) {
        bounds.push_back(from);
    }

    if (const Value to = getTo()) {
        bounds.push_back(to);
    }

    return speculatableWhen(llvm::all_of(bounds, holdsOnlyIntegers));
}

Speculation::Speculatability Substring::getSpeculatability() {
    return speculatableOverCharacters(getOperation());
}

Speculation::Speculatability Replace::getSpeculatability() {
    const bool readsStrings = llvm::all_of(getOperation()->getOperands(), [](Value operand) {
        return isa<storage::StringType>(columnElement(operand));
    });

    return speculatableWhen(readsStrings);
}

Speculation::Speculatability PointDistance::getSpeculatability() {
    return speculatableWhenReadingPoints(getOperation());
}

Speculation::Speculatability PointWithinBBox::getSpeculatability() {
    return speculatableWhenReadingPoints(getOperation());
}

Speculation::Speculatability Split::getSpeculatability() {
    const bool readsAString = isa<storage::StringType>(columnElement(getString()));

    const Type delimiter = columnElement(getDelimiter());
    const storage::ListType delimiterList = dyn_cast<storage::ListType>(delimiter);
    const Type delimiterElement = delimiterList ? delimiterList.getElementType() : delimiter;
    const bool splitsOnStrings = isa<storage::StringType>(delimiterElement);

    return speculatableWhen(readsAString && splitsOnStrings);
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

double GetNodeLabelSet::getRowCost() {
    return STORAGE_READ_ROW_COST;
}

double GetEdgeTypes::getRowCost() {
    return STORAGE_READ_ROW_COST;
}

double GetNodeProperties::getRowCost() {
    return STORAGE_READ_ROW_COST;
}

double GetEdgeProperties::getRowCost() {
    return STORAGE_READ_ROW_COST;
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

double MakeList::getRowCost() {
    return CONTAINER_BUILD_ROW_COST;
}

double MakeMap::getRowCost() {
    return CONTAINER_BUILD_ROW_COST;
}

double MergeMaps::getRowCost() {
    return CONTAINER_BUILD_ROW_COST;
}

double Range::getRowCost() {
    return CONTAINER_BUILD_ROW_COST;
}

double ListSlice::getRowCost() {
    return CONTAINER_BUILD_ROW_COST;
}

double Labels::getRowCost() {
    return STORAGE_READ_ROW_COST + CONTAINER_BUILD_ROW_COST;
}

double EdgeType::getRowCost() {
    return STORAGE_READ_ROW_COST;
}

double Keys::getRowCost() {
    return STORAGE_READ_ROW_COST + CONTAINER_BUILD_ROW_COST;
}

double PropertyMap::getRowCost() {
    return STORAGE_READ_ROW_COST + CONTAINER_BUILD_ROW_COST;
}

double StartNode::getRowCost() {
    return STORAGE_READ_ROW_COST;
}

double EndNode::getRowCost() {
    return STORAGE_READ_ROW_COST;
}

double CosineSimilarity::getRowCost() {
    return VECTOR_FUNCTION_ROW_COST;
}

double EuclideanDistance::getRowCost() {
    return VECTOR_FUNCTION_ROW_COST;
}
