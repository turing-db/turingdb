#include "DBLowering.h"

#include <mlir/IR/Location.h>
#include <optional>
#include <string>
#include <string_view>

#include <spdlog/fmt/fmt.h>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"
#include "DBLoweringChunkTypes.h"
#include "DBLoweringRowFlow.h"

#include "DBColumnElement.h"
#include "IRConstantColumn.h"

#include "metadata/PropertyType.h"

#include "IRValueTypes.h"

#include "IRException.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// The name a result column's value type goes out under. A rejection is read by whoever
// wrote the query, so it names the type the value has rather than the chunk spelling it
// is carried in.
void describeColumnType(mlir::Type chunkType, std::string& out) {
    out.clear();

    mlir::Type element = mlir::cast<nl::ChunkType>(chunkType).getElementType();
    if (const auto nullable = mlir::dyn_cast<storage::NullableType>(element)) {
        element = nullable.getValueType();
    }

    if (mlir::isa<storage::StringType>(element)) {
        out = "String";
    } else if (mlir::isa<storage::BoolType>(element)) {
        out = "Bool";
    } else if (mlir::isa<mlir::NoneType>(element)) {
        out = "Null";
    } else if (mlir::isa<mlir::Float64Type>(element)) {
        out = "Double";
    } else if (mlir::isa<storage::NodeIDType>(element)) {
        out = "Node";
    } else if (mlir::isa<storage::EdgeIDType>(element)) {
        out = "Edge";
    } else if (mlir::isa<storage::ListType>(element)) {
        out = "List";
    } else if (mlir::isa<storage::EmbeddingType>(element)) {
        out = "Embedding";
    } else if (mlir::isa<storage::PathType>(element)) {
        out = "Path";
    } else if (const auto integer = mlir::dyn_cast<mlir::IntegerType>(element)) {
        if (integer.getWidth() == 1) {
            out = "Bool";
        } else if (integer.isUnsigned()) {
            out = "UInt64";
        } else {
            out = "Int64";
        }
    } else {
        llvm::raw_string_ostream stream(out);
        stream << element;
    }
}

// Whether a value of @param element can be held as a tagged cell
bool holdsAsTaggedCell(mlir::Type element) {
    const bool isContainer = mlir::isa<storage::ListType, storage::MapType>(element);
    const bool isEntityOrCell = mlir::isa<storage::NodeIDType, storage::EdgeIDType, storage::AnyType>(element);

    return isContainer
        || isEntityOrCell
        || storage::namesASchemaID(element)
        || elementTypeToValueType(element) != ValueType::Invalid;
}

}

// Each branch is lowered as a program of its own rooted in the entry block, so its loops
// are appended after the ones the branch before it opened and the sink sees one branch's
// rows and then the next. What a branch binds is unreachable from the next, so the
// innermost-loop record is cleared between them rather than carried over.
void DBLowering::lowerUnion(mlir::db::Union unionOp) {
    if (unionOp.getNumResults() > 0) {
        lowerUnionResults(unionOp);
        return;
    }

    const mlir::MutableArrayRef<mlir::Region> branches = unionOp.getBranches();

    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;

    llvm::SmallVector<nl::Output, 4> branchOutputs;

    for (mlir::Region& branch : branches) {
        _rootBlock = _entryBlock;
        _innermostLoopBody = nullptr;
        _innermostCardinality = mlir::Value();

        mlir::db::Output branchOutput = mlir::cast<mlir::db::Output>(branch.front().back());

        for (mlir::Operation& operation : branch.front()) {
            llvm::SmallVector<std::pair<mlir::Value, mlir::Value>, 4> replacedMappings;
            convertUnionResultChunks(operation, branchOutput.getColumns(), replacedMappings);

            if (&operation == branchOutput.getOperation()) {
                branchOutputs.push_back(lowerOutput(branchOutput));
            } else {
                lowerOperation(operation);
            }

            for (const auto& [column, chunk] : replacedMappings) {
                _valueMap[column] = chunk;
            }
        }
    }

    llvm::SmallVector<mlir::MutableOperandRange, 4> branchColumns;
    for (nl::Output branchOutput : branchOutputs) {
        branchColumns.push_back(branchOutput.getColumnsMutable());
    }

    nl::Output firstOutput = branchOutputs.front();
    const std::optional<mlir::ArrayAttr> columnNames = firstOutput.getColumnNames();

    reconcileBranchResultTypes(branchColumns, columnNames.value_or(mlir::ArrayAttr()));

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;
}

// The branches of a union inside a CALL body feed the rest of the body, which is lowered
// once, so their rows meet in an accumulator: each branch is rooted in the block the body
// runs over, collects what it yields there, and the loop draining it is where the body
// goes on from.
void DBLowering::lowerUnionResults(mlir::db::Union unionOp) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::Block* const root = _rootBlock;

    _builder.setInsertionPointToStart(root);
    const mlir::Value state = _builder.create<nl::RowBuffer>(loc).getState();

    llvm::SmallVector<nl::RowCollect, 4> collects;

    for (mlir::Region& branch : unionOp.getBranches()) {
        lowerCollectedBranch(branch, root, state, collects);
    }

    drainCollectedBranches(collects, state, root, unionOp.getOperation());
}

void DBLowering::lowerConditional(mlir::db::Conditional conditional) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType maskType = nl::ChunkType::get(context, storage::BoolType::get(context));

    mlir::Block* const root = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    mlir::Block* const previousConstantComputationBlock = _constantComputationBlock;

    const bool returning = conditional.getNumResults() > 0;

    mlir::Value state;
    if (returning) {
        _builder.setInsertionPointToStart(root);
        state = _builder.create<nl::RowBuffer>(loc).getState();
    }

    _constantComputationBlock = root;

    llvm::SmallVector<mlir::Value, 4> readColumns;
    collectRowColumnsReadByBranches(conditional, readColumns);

    llvm::SmallVector<mlir::Value, 8> outerChunks;
    for (const mlir::Value column : readColumns) {
        outerChunks.push_back(mapValue(column));
    }

    // Codegen hands a WHEN its rows through the columns its branches read, so one reading
    // none runs once: the loop open before it may be a set a pass scanned ahead of it
    llvm::SmallVector<mlir::Value, 8> reaching(outerChunks.begin(), outerChunks.end());
    mlir::Value reachingCardinality = reaching.empty() ? mlir::Value() : reaching.front();

    const mlir::MutableArrayRef<mlir::Region> regions = conditional.getBranches();
    const size_t conditionCount = regions.size() / 2;
    const bool hasDefault = regions.size() % 2 == 1;

    llvm::SmallVector<nl::RowCollect, 4> collects;

    for (size_t conditionIndex = 0; conditionIndex < conditionCount; conditionIndex++) {
        for (size_t columnIndex = 0; columnIndex < readColumns.size(); columnIndex++) {
            _valueMap[readColumns[columnIndex]] = reaching[columnIndex];
        }

        const mlir::Value condition = lowerBlockOverRows(regions[2 * conditionIndex].front(), reachingCardinality);

        llvm::SmallVector<mlir::Value, 8> taken;
        filterCaseRows(condition, reaching, taken);
        lowerConditionalBranch(regions[2 * conditionIndex + 1], readColumns, taken, state, collects);

        const bool rowsFallThrough = hasDefault || conditionIndex + 1 < conditionCount;
        if (!rowsFallThrough) {
            break;
        }

        setInsertionForUnaryOp(condition);
        const mlir::Value fallsThrough = _builder.create<nl::IsNotTrue>(loc, maskType, condition).getResult();

        llvm::SmallVector<mlir::Value, 8> rest;
        filterCaseRows(fallsThrough, reaching, rest);

        reachingCardinality = rest.front();
        reaching.assign(rest.begin() + 1, rest.end());
    }

    if (hasDefault) {
        llvm::SmallVector<mlir::Value, 8> taken {reachingCardinality};
        taken.append(reaching.begin(), reaching.end());

        lowerConditionalBranch(regions.back(), readColumns, taken, state, collects);
    }

    for (size_t columnIndex = 0; columnIndex < readColumns.size(); columnIndex++) {
        _valueMap[readColumns[columnIndex]] = outerChunks[columnIndex];
    }

    _constantComputationBlock = previousConstantComputationBlock;

    if (!returning) {
        _innermostLoopBody = previousInnermostLoopBody;
        _innermostCardinality = previousInnermostCardinality;

        setInsertionInto(root);
        return;
    }

    drainCollectedBranches(collects, state, root, conditional.getOperation());
}

void DBLowering::lowerConditionalBranch(mlir::Region& branch,
                                        llvm::ArrayRef<mlir::Value> readColumns,
                                        llvm::ArrayRef<mlir::Value> taken,
                                        mlir::Value state,
                                        llvm::SmallVectorImpl<nl::RowCollect>& collects) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::Block* const root = _rootBlock;

    setInsertionInto(deepestOwnerBlock(taken, root));
    nl::EachRow eachRow = _builder.create<nl::EachRow>(loc, taken);
    nl::For branchLoop = _builder.create<nl::For>(loc, eachRow.getResult(), mlir::Value());
    mlir::Block* const branchRoot = branchLoop.getBody();

    for (size_t columnIndex = 0; columnIndex < readColumns.size(); columnIndex++) {
        const unsigned argumentIndex = static_cast<unsigned>(columnIndex + 1);
        _valueMap[readColumns[columnIndex]] = branchRoot->getArgument(argumentIndex);
    }

    _rootBlock = branchRoot;
    _constantComputationBlock = branchRoot;

    lowerCollectedBranch(branch, branchRoot, state, collects);

    _rootBlock = root;
    _constantComputationBlock = root;
}

void DBLowering::lowerCollectedBranch(mlir::Region& branch,
                                      mlir::Block* root,
                                      mlir::Value state,
                                      llvm::SmallVectorImpl<nl::RowCollect>& collects) {
    const mlir::Location loc = _builder.getUnknownLoc();

    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    mlir::db::Yield branchYield = mlir::cast<mlir::db::Yield>(branch.front().back());

    for (mlir::Operation& operation : branch.front()) {
        llvm::SmallVector<std::pair<mlir::Value, mlir::Value>, 4> replacedMappings;
        convertUnionResultChunks(operation, branchYield.getColumns(), replacedMappings);

        if (&operation != branchYield.getOperation()) {
            lowerOperation(operation);
        } else if (state) {
            llvm::SmallVector<mlir::Value, 4> chunks;
            for (const mlir::Value column : branchYield.getColumns()) {
                chunks.push_back(mapValue(column));
            }

            rowAlignBufferedChunks(chunks);

            // A branch of constants alone lays them out where they are bound, above the
            // root, which the collect must still run once per step of
            setInsertionInto(deepestOwnerBlock(chunks, root));
            collects.push_back(_builder.create<nl::RowCollect>(loc, state, chunks));
        }

        for (const auto& [column, chunk] : replacedMappings) {
            _valueMap[column] = chunk;
        }
    }
}

void DBLowering::drainCollectedBranches(llvm::ArrayRef<nl::RowCollect> collects,
                                        mlir::Value state,
                                        mlir::Block* root,
                                        mlir::Operation* holder) {
    const mlir::Location loc = _builder.getUnknownLoc();

    llvm::SmallVector<mlir::MutableOperandRange, 4> branchColumns;
    for (nl::RowCollect collect : collects) {
        branchColumns.push_back(collect.getColumnsMutable());
    }

    reconcileBranchResultTypes(branchColumns, mlir::ArrayAttr());

    nl::RowCollect firstCollect = collects.front();
    const llvm::SmallVector<mlir::Type, 4> chunkTypes(firstCollect.getColumns().getTypes());
    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), chunkTypes);

    setInsertionInto(root);
    nl::RowDrain drain = _builder.create<nl::RowDrain>(loc, iteratorType, state);

    buildLoopForSource(drain.getResult(), holder);
}

void DBLowering::collectRowColumnsReadByBranches(mlir::db::Conditional conditional,
                                                 llvm::SmallVectorImpl<mlir::Value>& columns) {
    mlir::Operation* const holder = conditional.getOperation();
    llvm::SmallPtrSet<mlir::Value, 8> seen;

    holder->walk([&](mlir::Operation* operation) {
        if (operation == holder) {
            return;
        }

        for (mlir::OpOperand& use : operation->getOpOperands()) {
            mlir::Value operand = use.get();
            const bool readFromAbove = !holder->isAncestor(operand.getParentRegion()->getParentOp());
            const bool isColumn = mlir::isa<mlir::db::ColumnType>(operand.getType());
            if (!readFromAbove || !isColumn || readsAheadOfTheRows(use) || seen.contains(operand)) {
                continue;
            }

            seen.insert(operand);

            if (!yieldsConstantColumn(mapValue(operand), _constantColumns)) {
                columns.push_back(operand);
            }
        }
    });
}

// A dedup keys a row on the bytes of the chunk it is handed, so a branch's result columns
// are converted on their way into it rather than on their way out: a count keyed as a
// plain ui64 and a property keyed as a nullable i64 spell the same number two different
// ways, and the duplicate the union exists to drop survives.
void DBLowering::convertUnionResultChunks(mlir::Operation& operation,
                                          mlir::OperandRange resultColumns,
                                          llvm::SmallVectorImpl<std::pair<mlir::Value, mlir::Value>>& replacedMappings) {
    llvm::SmallVector<mlir::Value, 4> columns;

    if (mlir::isa<mlir::db::Output, mlir::db::Yield>(operation)) {
        columns.assign(resultColumns.begin(), resultColumns.end());
    } else if (mlir::db::RemoveDuplicates dedup = mlir::dyn_cast<mlir::db::RemoveDuplicates>(operation)) {
        const mlir::OperandRange dedupColumns = dedup.getColumns();
        const mlir::ResultRange results = dedup.getResults();

        for (size_t columnIndex = 0; columnIndex < results.size(); columnIndex++) {
            if (llvm::is_contained(resultColumns, results[columnIndex])) {
                columns.push_back(dedupColumns[columnIndex]);
            }
        }
    }

    if (columns.empty()) {
        return;
    }

    // A constant is converted once it holds rows: a union inside a CALL body lays its
    // constants out here rather than in codegen
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }

    rowAlignBufferedChunks(chunks);

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        const mlir::Value column = columns[columnIndex];
        replacedMappings.emplace_back(column, mapValue(column));
        _valueMap[column] = unionColumnChunk(chunks[columnIndex]);
    }
}

// A result column carries one type for the whole result, which is only known here: a
// property fetch is typed none until its name is resolved against the schema. A branch not
// holding that type is converted to it before the dedup the branches share, so that every
// branch spells a row's key the one way.
void DBLowering::reconcileBranchResultTypes(llvm::ArrayRef<mlir::MutableOperandRange> branchColumns,
                                            mlir::ArrayAttr columnNames) {
    const size_t columnCount = branchColumns.front().size();

    for (size_t columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        const mlir::Type columnType = unionResultType(branchColumns, columnIndex, columnNames);

        for (const mlir::MutableOperandRange& columns : branchColumns) {
            mlir::OpOperand& operand = columns[static_cast<unsigned>(columnIndex)];
            if (operand.get().getType() != columnType) {
                retypeBranchColumn(operand, columnType);
            }
        }
    }
}

// The type the branches of a result column agree on, when every one holds it and it holds
// a branch's null. Where they disagree, each row keeps the type of its value, as the rows a
// CASE selects do: the column holds the element their values select into. Where every
// branch is the null literal, the first branch's untyped null is the column.
mlir::Type DBLowering::unionResultType(llvm::ArrayRef<mlir::MutableOperandRange> branchColumns,
                                       size_t columnIndex,
                                       mlir::ArrayAttr columnNames) {
    const unsigned operandIndex = static_cast<unsigned>(columnIndex);

    mlir::Type sharedType;
    mlir::Type element;
    bool branchesAgree = true;
    bool holdsANull = false;

    for (const mlir::MutableOperandRange& columns : branchColumns) {
        const mlir::Type branchType = columns[operandIndex].get().getType();
        if (isUntypedNullChunk(branchType)) {
            holdsANull = true;
            continue;
        }

        const mlir::Type branchElement = chunkValueElement(_builder, branchType);
        holdsANull = holdsANull || isNullableChunk(branchType);

        if (!sharedType) {
            sharedType = branchType;
            element = branchElement;
            continue;
        }

        const mlir::Type sharedElement = chunkValueElement(_builder, sharedType);
        const bool holdsCells = holdsAsTaggedCell(branchElement) && holdsAsTaggedCell(sharedElement);
        if (branchElement != sharedElement && !holdsCells) {
            throwOnDisagreeingBranchTypes(branchType, sharedType, columnNames, columnIndex);
        }

        branchesAgree = branchesAgree && branchType == sharedType;

        const mlir::Type selected = selectedValueElement(_builder, element, branchElement);
        element = selected ? selected : storage::AnyType::get(_builder.getContext());
    }

    if (!sharedType) {
        return branchColumns.front()[operandIndex].get().getType();
    } else if (branchesAgree && (!holdsANull || holdsItsNull(sharedType))) {
        return sharedType;
    }

    const bool isContainer = mlir::isa<storage::ListType, storage::MapType>(element);
    if (isContainer && !holdsANull) {
        return nl::ChunkType::get(_builder.getContext(), element);
    }

    return caseResultChunkType(element);
}

// A branch that dedups keys its rows on the chunks it is handed, so its column is converted
// ahead of its filter, and the filter's survivors take the converted type
void DBLowering::retypeBranchColumn(mlir::OpOperand& operand, mlir::Type chunkType) {
    const mlir::Value chunk = operand.get();

    nl::DistinctFilter filter = chunk.getDefiningOp<nl::DistinctFilter>();
    if (!filter) {
        operand.set(branchChunkAs(chunk, chunkType));
        return;
    }

    mlir::OpResult survivors = mlir::cast<mlir::OpResult>(chunk);
    mlir::OpOperand& key = filter.getColumnsMutable()[survivors.getResultNumber()];

    key.set(branchChunkAs(key.get(), chunkType));
    survivors.setType(chunkType);
}

// @param chunk as a column of @param chunkType, one unionResultType answers: a column of
// tagged cells, or the branch's own values in the shape that holds the null of another
mlir::Value DBLowering::branchChunkAs(mlir::Value chunk, mlir::Type chunkType) {
    if (chunk.getType() == chunkType) {
        return chunk;
    } else if (isUntypedNullChunk(chunk.getType())) {
        return typedNullChunk(chunk, chunkType);
    }

    const mlir::Type resultElement = mlir::cast<nl::ChunkType>(chunkType).getElementType();
    if (mlir::isa<storage::AnyType>(resultElement)) {
        return toAnyChunk(chunk, chunkType);
    }

    const auto nullableResult = mlir::dyn_cast<storage::NullableType>(resultElement);
    const mlir::Type resultValue = nullableResult ? nullableResult.getValueType() : resultElement;

    // Two lists of different elements are the list they join to, and two integers the one
    // integer Cypher has
    mlir::Value converted = chunk;
    const bool convertsElement = chunkValueElement(_builder, converted.getType()) != resultValue;

    if (convertsElement && mlir::isa<storage::ListType>(resultValue)) {
        mlir::MLIRContext* const context = _builder.getContext();
        const bool nullableList = isNullableChunk(converted.getType());
        const mlir::Type listElement = nullableList ? mlir::Type(storage::NullableType::get(context, resultValue)) : resultValue;

        converted = toAnyChunk(converted, nl::ChunkType::get(context, listElement));
    } else if (convertsElement) {
        converted = chunkAsElement(converted, resultValue);
    }

    if (!nullableResult) {
        return converted;
    }

    return paddedColumnChunk(converted);
}

void DBLowering::throwOnDisagreeingBranchTypes(mlir::Type branchType,
                                               mlir::Type resultType,
                                               mlir::ArrayAttr columnNames,
                                               size_t columnIndex) {
    std::string branchName;
    describeColumnType(branchType, branchName);

    std::string resultName;
    describeColumnType(resultType, resultName);

    std::string column = "A column";
    if (columnNames) {
        const llvm::StringRef columnName = mlir::cast<mlir::StringAttr>(columnNames[columnIndex]).getValue();
        column = fmt::format("Column '{}'", std::string_view {columnName.data(), columnName.size()});
    }

    throw IRException(fmt::format("{} of a UNION or a WHEN cannot hold both {} and {} values",
                                  column,
                                  branchName,
                                  resultName));
}

// The one column a union result carries, whichever branch filled it. A count is an
// unsigned tally that is never null and a property a nullable signed value, and the two
// are one Cypher INTEGER, so a scalar reaches the column as the nullable signed form. An
// entity, a path, a list and an embedding have one spelling already and are left alone.
mlir::Value DBLowering::unionColumnChunk(mlir::Value chunk) {
    const mlir::Type element = chunkValueElement(_builder, chunk.getType());

    const bool isString = mlir::isa<storage::StringType>(element);
    const bool isDouble = mlir::isa<mlir::Float64Type>(element);
    const mlir::IntegerType integerType = mlir::dyn_cast<mlir::IntegerType>(element);

    if (!isString && !isDouble && !integerType) {
        return chunk;
    }

    // Wrapped before it is converted, and not instead: the kernel a conversion selects
    // reads the column it was handed, so converting a plain chunk would allocate a plain
    // column under a type naming a nullable one
    const mlir::Value nullableChunk = nullableValueChunk(chunk);

    const bool isUnsignedInteger = integerType && integerType.isUnsigned() && integerType.getWidth() == 64;
    if (!isUnsignedInteger) {
        return nullableChunk;
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const storage::NullableType signedElement = storage::NullableType::get(context, _builder.getIntegerType(64));
    const nl::ChunkType signedChunk = nl::ChunkType::get(context, signedElement);

    mlir::OpBuilder::InsertionGuard guard(_builder);
    setInsertionForUnaryOp(nullableChunk);

    return _builder.create<nl::ToInteger>(_builder.getUnknownLoc(), signedChunk, nullableChunk).getResult();
}
