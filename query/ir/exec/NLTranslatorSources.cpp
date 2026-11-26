#include "NLTranslator.h"

#include <vector>
#include <span>
#include <string_view>

#include <spdlog/fmt/bundled/format.h>

#include "NLOps.h"

#include "mlir/IR/Block.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureManager.h"
#include "columns/ColumnConst.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"

#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

void NLTranslator::translateUnwindConstLoop(const IteratorConfig& config,
                                            mlir::Block& loopBody,
                                            NLLimitState* limit,
                                            NLStmtContainer* body) {
    // An unwind_const binds a single value chunk: the loop emits one element per row.
    const mlir::Value valueChunk = loopBody.getArgument(0);
    const mlir::Type elementType = mlir::cast<nl::ChunkType>(valueChunk.getType()).getElementType();

    // The chunk element type is the homogeneity verdict: a !storage.any chunk is the
    // type-erased column of tagged scalars; a nullable value chunk is a homogeneous
    // column of that one value type, the shape every other value-chunk consumer reads.
    const bool heterogeneous = llvm::isa<storage::AnyType>(elementType);

    Column* output {nullptr};
    ValueType valueType {ValueType::Invalid};

    if (heterogeneous) {
        output = allocCellColumn();
    } else {
        valueType = nullableChunkValueType(valueChunk.getType());
        output = allocOptColumnForValueType(valueType);
    }

    _valueSlots[valueChunk] = output;

    NLUnwindConstLoopData* loopData = _program->allocFunctionData<NLUnwindConstLoopData>(output,
                                                                                        config._list,
                                                                                        heterogeneous,
                                                                                        valueType);
    loopData->setLimit(limit);

    body->emplaceStmt(&NLExecutor::runUnwindConstLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateLoadCSVLoop(const IteratorConfig& config,
                                        mlir::Block& loopBody,
                                        NLLimitState* limit,
                                        NLStmtContainer* body) {
    // A CSV load binds one string chunk per field it produces. The parser fills a
    // row of field columns, so the chunks are gathered into one - the chunk a downstream
    // op reads is the very column the parser wrote.
    ColumnStringTable* const row = _memory->alloc<ColumnStringTable>();

    for (const mlir::Value fieldChunk : loopBody.getArguments()) {
        Column* const field = allocColumn(fieldChunk);
        row->addFieldColumn(static_cast<ColumnStringTable::StringColumn*>(field));
    }

    const std::string_view path {config._csvPath.data(), config._csvPath.size()};

    NLLoadCSVLoopData* loopData = _program->allocFunctionData<NLLoadCSVLoopData>(row,
                                                                                &_program->allocStepBuffers()->stringBuffer(),
                                                                                path,
                                                                                config._csvHasHeaders,
                                                                                config._csvSkipOnError);
    loopData->setLimit(limit);

    const mlir::Value boundPath = config._csvBoundPath;
    if (boundPath) {
        const Column* column = getColumn(boundPath);
        bioassert(column->getKind() == ColumnConst<types::String::Primitive>::staticKind(), "A bound path holds one value for the run");
        loopData->setBoundPath(static_cast<const ColumnConst<types::String::Primitive>*>(column));
    }

    for (const mlir::Attribute field : config._csvFields) {
        if (const auto header = mlir::dyn_cast<mlir::StringAttr>(field)) {
            const llvm::StringRef name = header.getValue();
            loopData->addField({._header = std::string_view {name.data(), name.size()},
                                ._byHeader = true});
        } else {
            const auto index = mlir::cast<mlir::IntegerAttr>(field);
            loopData->addField({._index = index.getValue().getZExtValue()});
        }
    }

    body->emplaceStmt(&NLExecutor::runLoadCSVLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateVectorSearchLoop(const IteratorConfig& config,
                                             mlir::Block& loopBody,
                                             NLLimitState* limit,
                                             NLStmtContainer* body) {
    // A vector search binds two chunks: the neighbour nodes and the distances they scored.
    const mlir::Value idChunk = loopBody.getArgument(0);
    const mlir::Value scoreChunk = loopBody.getArgument(1);

    ColumnNodeIDs* const ids = static_cast<ColumnNodeIDs*>(allocColumn(idChunk));
    Column* const scores = allocColumn(scoreChunk);

    const std::string_view indexName {config._indexName.data(), config._indexName.size()};
    const std::span<const float> queryVector {config._queryVector.data(), config._queryVector.size()};

    NLVectorSearchLoopData* loopData = _program->allocFunctionData<NLVectorSearchLoopData>(ids,
                                                                                          scores,
                                                                                          indexName,
                                                                                          config._neighbourCount,
                                                                                          queryVector);
    loopData->setLimit(limit);

    const mlir::Value boundIndexName = config._boundIndexName;
    const mlir::Value boundCount = config._boundNeighbourCount;
    const mlir::Value boundQueryVector = config._boundQueryVector;

    loopData->setBoundIndexName(boundIndexName ? getColumn(boundIndexName) : nullptr);
    loopData->setBoundNeighbourCount(boundCount ? getColumn(boundCount) : nullptr);
    loopData->setBoundQueryVector(boundQueryVector ? getColumn(boundQueryVector) : nullptr);

    body->emplaceStmt(&NLExecutor::runVectorSearchLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

NLUnwindElementEmitFunction NLTranslator::selectListUnwindEmit(mlir::Type chunkType, bool sourceIsNullable) {
    const mlir::Type elementType = mlir::cast<nl::ChunkType>(chunkType).getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectListUnwindValueEmit(sourceIsNullable, chunkKindFromNullableValue(nullableType.getValueType()));
    } else if (mlir::isa<storage::NodeIDType>(elementType)) {
        return NLExecutor::selectListUnwindNodeEmit(sourceIsNullable);
    } else if (mlir::isa<storage::EdgeIDType>(elementType)) {
        return NLExecutor::selectListUnwindEdgeEmit(sourceIsNullable);
    } else if (mlir::isa<storage::PathType>(elementType)) {
        return NLExecutor::selectListUnwindPathEmit(sourceIsNullable);
    }

    return NLExecutor::selectListUnwindElementEmit(sourceIsNullable);
}

// A column whose cells hold more than the element - a list, or a tagged scalar that may
// itself be a list - drains through its own emit. A scalar column already holds one
// element per cell, so its element column is the source's own rows gathered by the step:
// a carried column like the rest, and no drain here.
void NLTranslator::selectElementDrain(mlir::Type sourceElement,
                                      mlir::Type elementChunkType,
                                      NLUnwindElementCountFunction& elementCount,
                                      NLUnwindElementEmitFunction& elementEmit) {
    const auto sourceNullable = mlir::dyn_cast<storage::NullableType>(sourceElement);
    const bool drainsNullableList = sourceNullable && llvm::isa<storage::ListType>(sourceNullable.getValueType());

    elementEmit = nullptr;

    if (llvm::isa<storage::ListType>(sourceElement) || drainsNullableList) {
        elementCount = NLExecutor::selectListUnwindElementCount(drainsNullableList);
        elementEmit = selectListUnwindEmit(elementChunkType, drainsNullableList);
    } else if (llvm::isa<storage::AnyType>(sourceElement)) {
        elementCount = NLExecutor::selectTaggedUnwindElementCount();
        elementEmit = NLExecutor::selectTaggedUnwindElementEmit();
    } else if (sourceNullable) {
        elementCount = NLExecutor::selectOptUnwindElementCount(chunkKindFromNullableValue(sourceNullable.getValueType()));
    } else {
        elementCount = NLExecutor::selectValueUnwindElementCount();
    }
}

NLCellAbsentFunction NLTranslator::selectCellAbsent(mlir::Type sourceElement) {
    if (llvm::isa<storage::AnyType>(sourceElement)) {
        return NLExecutor::selectTaggedCellAbsent();
    } else if (isNullableList(sourceElement)) {
        return NLExecutor::selectListCellAbsent();
    }

    const auto sourceNullable = mlir::dyn_cast<storage::NullableType>(sourceElement);
    if (!sourceNullable) {
        return NLExecutor::selectPresentCell();
    }

    return NLExecutor::selectValueCellAbsent(chunkKindFromNullableValue(sourceNullable.getValueType()));
}

void NLTranslator::translateProcedure(nl::Procedure procedureOp, NLStmtContainer* body) {
    const llvm::StringRef name = procedureOp.getName();

    if (!_procedureContext) {
        throw IRException(fmt::format("nl.procedure of '{}' requires a procedure context, but the "
                                      "translator was created without one",
                                      name.str()));
    }

    const ProcedureManager* procedures = _procedureContext->getProcedures();
    if (!procedures) {
        throw IRException("The procedure context carries no procedure registry to resolve "
                          "nl.procedure against");
    }

    // A procedure fills at most a chunk's worth of rows per call, reading that budget
    // off the context: a zero budget leaves a drive loop asking an exhausted-looking
    // procedure for rows forever, so reject the unset context here rather than hang.
    if (_procedureContext->getChunkSize() == 0) {
        throw IRException("The procedure context carries no chunk size, so a procedure would be "
                          "asked for chunks of no rows");
    }

    const Procedure* procedure = procedures->getProcedure(std::string_view(name.data(), name.size()));
    if (!procedure) {
        throw IRException(fmt::format("Procedure '{}' does not exist", name.str()));
    }

    // A procedure keeps its own state - iterators, tallies - in the data its alloc
    // callback produces, and every callback runs through the execute entry point, so
    // a procedure missing either cannot be called at all.
    const Procedure::AllocCallback alloc = procedure->getAllocCallback();
    if (!alloc || !procedure->getExecCallback()) {
        throw IRException(fmt::format("Procedure '{}' has no alloc or execute callback", name.str()));
    }

    // The slots the procedure reads its arguments from and writes its return values
    // into. Every declared value gets a slot, so a return value this call does not
    // yield stays null and the procedure skips it.
    ProcedureData* procedureData = alloc();
    procedureData->resizeInputColumns(procedure->argumentTypes().size());
    procedureData->resizeReturnColumns(procedure->returnValues().size());

    // The runtime call owns that data from here on - its dealloc callback releases it
    // - and is mapped to the handle, so every op that names the handle drives the same
    // procedure.
    NLProcedureState* state = _program->allocProcedureState(procedure, procedureData, _procedureContext);
    _procedureStates[procedureOp.getState()] = state;

    // Resolve each yielded name to the procedure's own return value index once here,
    // so the ops that bind columns work off indices rather than names.
    for (const mlir::Attribute yield : procedureOp.getYields()) {
        const llvm::StringRef yieldName = mlir::cast<mlir::StringAttr>(yield).getValue();

        state->addYieldIndex(procedure->getReturnValueIndex(std::string_view(yieldName.data(), yieldName.size())));
    }
}

void NLTranslator::bindProcedureInputs(NLProcedureState* state, mlir::ValueRange inputs) {
    // Operand i is argument i, so each argument chunk lands in the slot the procedure
    // reads that argument from. The chunks are the enclosing loop's variables, refilled
    // in place each step, so binding them once here holds for every step. A call that
    // stopped short of the trailing optional arguments leaves their slots at the null
    // the alloc sized them to, which is how the procedure reads an omitted one.
    const Procedure* procedure = state->getProcedure();
    const size_t argumentCount = procedure->argumentTypes().size();
    const size_t requiredCount = procedure->getRequiredArgumentCount();
    const bool tooFewArguments = inputs.size() < requiredCount;
    const bool tooManyArguments = inputs.size() > argumentCount;
    if (tooFewArguments || tooManyArguments) {
        throw IRException(fmt::format("A procedure call passes {} arguments, but the procedure "
                                      "declares {}, {} of them required",
                                      inputs.size(),
                                      argumentCount,
                                      requiredCount));
    }

    ProcedureData* procedureData = state->getData();
    for (size_t inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
        procedureData->setInputColumn(inputIndex, getColumn(inputs[inputIndex]));
    }
}

void NLTranslator::addProcedureCarriedColumns(const IteratorConfig& config,
                                              mlir::Block& loopBody,
                                              size_t yieldCount,
                                              NLProcedureLoopData* loopData) {
    const llvm::SmallVector<mlir::Value, 4>& carriedColumns = config._carriedColumns;
    const Procedure* procedure = loopData->getState()->getProcedure();

    // A call binding no return value has no row count for a carried row to be replicated
    // against, so nothing can ride through it - it is driven for what it does, not for
    // rows. The db verifier settles this, so reaching it here means hand-written nl IR.
    if (!carriedColumns.empty() && yieldCount == 0) {
        throw IRException(fmt::format("nl.procedure_init carries columns past '{}', which binds no "
                                      "return value, so they could not be aligned with anything",
                                      procedure->getFullName()));
    }

    // Only a procedure that declares it reports the input row behind each row it emits
    // can be carried past: that report is what the carried columns are rebuilt from.
    // Lowering settles this at plan time, so reaching it here means hand-written nl IR.
    if (!carriedColumns.empty() && !procedure->hasIndices()) {
        throw IRException(fmt::format("nl.procedure_init carries columns past '{}', but the "
                                      "procedure does not report the input row of the rows it emits",
                                      procedure->getFullName()));
    }

    for (size_t carriedIndex = 0; carriedIndex < carriedColumns.size(); carriedIndex++) {
        const mlir::Value carried = carriedColumns[carriedIndex];

        // A carried chunk comes back as a loop variable after the yields, and is
        // rebuilt into it each step - the call may repeat or drop its rows, so it
        // cannot be passed through in place.
        const auto loopVariableIndex = static_cast<unsigned>(yieldCount + carriedIndex);
        const mlir::Value loopVariable = loopBody.getArgument(loopVariableIndex);

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;

        const NLCarriedColumn column(getColumn(carried),
                                     output,
                                     selectGatherForChunkType(loopVariable.getType()));
        loopData->addCarriedColumn(column);
    }

    if (carriedColumns.empty()) {
        return;
    }

    // The procedure reports the input row behind each row it emits only when something
    // is carried past the call; hand it the map the loop gathers those columns through,
    // reserving a chunk so the reporting stays allocation-free.
    ColumnIndices* indices = loopData->getIndices();
    indices->reserve(_program->getChunkSize());

    ProcedureData* data = loopData->getState()->getData();
    IndexedProcedureData* indexedData = dynamic_cast<IndexedProcedureData*>(data);

    bioassert(indexedData,
              "Procedure '{}' is expected to have indices but has no indices data",
              procedure->getFullName());

    indexedData->setIndices(indices);
}

void NLTranslator::translateProcedureInitLoop(const IteratorConfig& config,
                                              mlir::Block& loopBody,
                                              NLLimitState* limit,
                                              NLStmtContainer* body) {
    NLProcedureState* state = config._procedureState;
    if (!state) {
        throw IRException("nl.procedure_init iterator must carry a procedure call");
    }

    bindProcedureInputs(state, config._procedureInputs);

    // For::verify binds one loop variable per iterator chunk, and a drive iterator's
    // chunks are the yielded return values followed by the carried ones. The yields are
    // the columns the procedure fills, so those loop variables are its result columns -
    // each step rewrites them in place, with no gather and no copy.
    const size_t yieldCount = state->yieldIndices().size();
    const size_t carriedCount = config._carriedColumns.size();
    if (loopBody.getNumArguments() != yieldCount + carriedCount) {
        throw IRException(fmt::format("An nl.procedure_init loop binds {} variables, but its call "
                                      "yields {} return values and carries {} columns",
                                      loopBody.getNumArguments(),
                                      yieldCount,
                                      carriedCount));
    }

    bindProcedureResults(state, loopBody.getArguments().take_front(yieldCount));

    NLProcedureLoopData* loopData = _program->allocFunctionData<NLProcedureLoopData>(state);
    loopData->setLimit(limit);

    // The carried columns come back as the trailing loop variables, rebuilt each step
    // from the input rows the procedure reports.
    addProcedureCarriedColumns(config, loopBody, yieldCount, loopData);

    body->emplaceStmt(&NLExecutor::runProcedureInitLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

NLProcedureState* NLTranslator::procedureStateFor(mlir::Value handle) const {
    const auto stateIt = _procedureStates.find(handle);
    if (stateIt == _procedureStates.end()) {
        throw IRException("procedure handle must be produced by an nl.procedure");
    }

    return stateIt->second;
}

void NLTranslator::bindProcedureResults(NLProcedureState* state, mlir::ValueRange chunks) {
    // One op binds the call's result columns - the drive loop - so a second one would
    // allocate a rival set of columns the procedure no longer writes into.
    if (!state->resultColumns().empty()) {
        throw IRException("A procedure call binds its result chunks once, but a second operation "
                          "names the same handle");
    }

    const std::vector<size_t>& yieldIndices = state->yieldIndices();
    if (chunks.size() != yieldIndices.size()) {
        throw IRException(fmt::format("A procedure call produces {} chunks, but its nl.procedure "
                                      "yields {} return values",
                                      chunks.size(),
                                      yieldIndices.size()));
    }

    const Procedure* procedure = state->getProcedure();
    ProcedureData* procedureData = state->getData();

    for (size_t yieldIndex = 0; yieldIndex < yieldIndices.size(); yieldIndex++) {
        const size_t returnIndex = yieldIndices[yieldIndex];
        Column* column = allocColumnForProcedureType(procedure->returnValues()[returnIndex]);

        // The procedure writes through the slot its own declaration order names; the
        // engine reads the same column through the chunk value, and the ordered list
        // on the call sizes each step's rows.
        procedureData->setReturnColumn(returnIndex, column);
        state->addResultColumn(column);
        _valueSlots[chunks[yieldIndex]] = column;
    }
}
