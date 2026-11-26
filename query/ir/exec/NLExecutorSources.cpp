#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>
#include <numeric>
#include <span>
#include <string>
#include <string_view>

#include <spdlog/fmt/bundled/format.h>

#include "ID.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "CellUtils.h"
#include "IteratedList.h"
#include "QueryTypeName.h"
#include "metadata/PropertyType.h"

#include "CSVParser.h"

#include "VecLibAccessor.h"
#include "VectorDatabase.h"
#include "VectorSearchQuery.h"
#include "VectorSearchResult.h"

#include "NLSystemContext.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLBoundConstant.h"
#include "NLColumnKernels.h"
#include "NLElementCursor.h"
#include "NLLoopSteps.h"
#include "NLRangeSpan.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

// The cursor an unwind_range loop walks: every (row, integer) pair the bounds span, in row
// order. It is local to the walk, so one nested in an outer loop restarts on every one of
// its steps.
class NLRangeCursor {
public:
    explicit NLRangeCursor(const NLUnwindRangeLoopData* loopData)
        : _start(loopData->getStart()),
        _end(loopData->getEnd()),
        _step(loopData->getStep()),
        _window(loopData->getWindow()),
        _rowCount(_start._column->size())
    {
        const auto isRowAligned = [this](const NLRangeBound& bound) {
            return !bound._column || bound._column->size() == _rowCount;
        };

        const bool boundsRowAligned = isRowAligned(_end) && isRowAligned(_step)
                                   && isRowAligned(_window._from) && isRowAligned(_window._to);
        bioassert(boundsRowAligned, "Bound columns of an unwind_range are not row-aligned.");

        openNextRow();
    }

    bool exhausted() const { return _row >= _rowCount; }

    // Takes the next @param chunkSize integers, or what is left of them, with the row each
    // one came from. @param values is null where nothing reads the integers.
    void fillChunk(size_t chunkSize, ColumnOptVector<types::Int64::Primitive>* values, std::vector<size_t>& rows) {
        rows.resize(chunkSize);
        if (values) {
            values->clear();
        }

        size_t filled = 0;
        while (filled < chunkSize && !exhausted()) {
            const size_t room = chunkSize - filled;
            const uint64_t remainingSteps = _span.getSteps() - _position;
            const bool finishesRow = remainingSteps < room;
            const size_t count = finishesRow ? static_cast<size_t>(remainingSteps) + 1 : room;

            if (values) {
                values->resizeValid(filled + count);
                std::vector<types::Int64::Primitive>& valuesRaw = values->getValues();

                for (size_t index = 0; index < count; index++) {
                    valuesRaw[filled + index] = _span.at(_position + index);
                }
            }

            std::fill_n(rows.begin() + filled, count, _row);
            filled += count;

            if (finishesRow) {
                _row++;
                openNextRow();
            } else {
                _position += count;
            }
        }

        rows.resize(filled);
    }

private:
    NLRangeBound _start;
    NLRangeBound _end;
    NLRangeBound _step;
    NLRangeWindowBounds _window;
    size_t _rowCount {0};
    size_t _row {0};
    uint64_t _position {0};
    NLRangeSpan _span;

    void openNextRow() {
        _position = 0;

        while (_row < _rowCount) {
            const bool spansIntegers = _span.read(_start, _end, _step, _row)
                                    && _span.keepWindow(_window, _row)
                                    && !_span.isEmpty();
            if (spansIntegers) {
                return;
            }

            _row++;
        }
    }
};

// Copy a slice of a ListView's tagged scalars straight into a
// ColumnVector<CellView> - the heterogeneous unwind's type-erased column.
void fillListElementChunk(Column* output, const ListView list, size_t offset, size_t rows) {
    ColumnVector<CellView>* typed = static_cast<ColumnVector<CellView>*>(output);
    const std::span<const CellView> elements = list.elements();

    std::vector<CellView>& raw = typed->getRaw();
    raw.assign(elements.begin() + offset, elements.begin() + offset + rows);
}

// Fill a slice of a ListView into a nullable value column, extracting each element as
// the homogeneous primitive. The homogeneous unwind's fast path, ported from
// UnwindProcessor::fillHomogeneous onto the ColumnOptVector every other value-chunk
// consumer reads; a null element names no type of its own and rides the column as an
// absent cell.
void fillHomogeneousChunk(Column* output, ValueType valueType, const ListView list, size_t offset, size_t rows) {
    const std::span<const CellView> elements = list.elements();

    const auto fill = [&]<SupportedType T>() {
        using Primitive = typename T::Primitive;

        ColumnOptVector<Primitive>* typed = static_cast<ColumnOptVector<Primitive>*>(output);

        resetForRowWrites(typed, rows);
        std::vector<Primitive>& typedRaw = typed->getValues();

        // CellView::getAs reinterprets the element's bytes as the primitive
        // without consulting its type tag, so check the tag against the one the
        // primitive is stored under: a homogeneous unwind whose column type disagrees
        // with its literals would otherwise read a value of the wrong type.
        constexpr CellTag expectedTag = TypeToCellTag<Primitive>::Tag;

        for (size_t index = 0; index < rows; index++) {
            const CellView element = elements[offset + index];
            const CellTag tag = element.getTag();

            if (tag == CellTag::Null) {
                typed->setNull(index);
                continue;
            }

            bioassert(tag == expectedTag, "Unwound element does not have the unwind's value type.");

            typedRaw[index] = element.getAs<Primitive>();
        }
    };

    ValueTypeDispatcher {valueType}.execute(fill);
}

// The list one row of an unwind source holds. A plain list column has one in every row; a
// nullable one - a list read back out of a property - has none where the property is
// absent, and those rows never reach an emit because the count function skips them.
ListView sourceList(const ColumnVector<ListView>* source, size_t row) {
    return source->getRaw()[row];
}

ListView sourceList(const ColumnOptVector<ListView>* source, size_t row) {
    return source->getValue(row);
}

// The rows one cell of a list column unwinds into: one per element, so an empty list
// contributes none.
size_t unwindListElementCount(const Column* source, size_t row) {
    const auto* lists = static_cast<const ColumnVector<ListView>*>(source);
    return (*lists)[row].size();
}

// The nullable sibling: an absent list unwinds into no row at all, as a null value does.
size_t unwindOptListElementCount(const Column* source, size_t row) {
    const ColumnOptVector<ListView>* lists = static_cast<const ColumnOptVector<ListView>*>(source);
    return lists->isValid(row) ? lists->getValue(row).size() : 0;
}

// The rows one cell of a nullable value column unwinds into: the single row a present
// value is, and none for a null - Cypher's UNWIND of a null.
template <typename Primitive>
size_t unwindOptElementCount(const Column* source, size_t row) {
    const auto* values = static_cast<const ColumnOptVector<Primitive>*>(source);
    return values->isValid(row) ? 1 : 0;
}

// The rows one cell of a column holding a value in every row unwinds into: the single
// row that value is.
size_t unwindValueElementCount(const Column* source, size_t row) {
    return 1;
}

// Copy the tagged elements one step covers out of a list column into the type-erased
// element column: each output row is the element at its position in the list its source
// row holds.
template <typename SourceColumn>
void unwindListElementEmit(const Column* source,
                           const ColumnVector<size_t>* rows,
                           const ColumnVector<size_t>* positions,
                           Column* output) {
    const SourceColumn* lists = static_cast<const SourceColumn*>(source);
    const std::vector<size_t>& rowsRaw = rows->getRaw();
    const std::vector<size_t>& positionsRaw = positions->getRaw();

    std::vector<CellView>& outputRaw = static_cast<ColumnVector<CellView>*>(output)->getRaw();
    outputRaw.resize(rowsRaw.size());

    for (size_t index = 0; index < rowsRaw.size(); index++) {
        outputRaw[index] = sourceList(lists, rowsRaw[index]).elements()[positionsRaw[index]];
    }
}

// Copy the elements one step covers out of a list column into a typed value column: each
// output row is the element at its position in the list its source row holds. The list's
// element type is the column's, so every element either carries that type or is the null
// a list built out of a column with an absent cell holds - the tag check is what holds a
// list whose elements disagree with both to that promise.
template <typename SourceColumn, typename Primitive>
void unwindListValueEmit(const Column* source,
                         const ColumnVector<size_t>* rows,
                         const ColumnVector<size_t>* positions,
                         Column* output) {
    const SourceColumn* lists = static_cast<const SourceColumn*>(source);
    const std::vector<size_t>& rowsRaw = rows->getRaw();
    const std::vector<size_t>& positionsRaw = positions->getRaw();

    ColumnOptVector<Primitive>* typedOutput = static_cast<ColumnOptVector<Primitive>*>(output);
    resetForRowWrites(typedOutput, rowsRaw.size());
    std::vector<Primitive>& outputRaw = typedOutput->getValues();

    constexpr CellTag expectedTag = TypeToCellTag<Primitive>::Tag;

    for (size_t index = 0; index < rowsRaw.size(); index++) {
        const CellView element = sourceList(lists, rowsRaw[index]).elements()[positionsRaw[index]];
        const CellTag tag = element.getTag();

        if (tag == CellTag::Null) {
            typedOutput->setNull(index);
            continue;
        }

        bioassert(tag == expectedTag, "Unwound element does not have the unwound list's value type.");

        outputRaw[index] = element.getAs<Primitive>();
    }
}

// The entity sibling of unwindListValueEmit: an ID column spells a null entity as an
// invalid ID, so the tagged null a list holds drains back into one rather than tripping
// the tag check.
template <typename SourceColumn, typename IDType>
void unwindListValidIDEmit(const Column* source,
                           const ColumnVector<size_t>* rows,
                           const ColumnVector<size_t>* positions,
                           Column* output) {
    const SourceColumn* lists = static_cast<const SourceColumn*>(source);
    const std::vector<size_t>& rowsRaw = rows->getRaw();
    const std::vector<size_t>& positionsRaw = positions->getRaw();

    std::vector<IDType>& outputRaw = static_cast<ColumnVector<IDType>*>(output)->getRaw();
    outputRaw.resize(rowsRaw.size());

    constexpr CellTag expectedTag = TypeToCellTag<IDType>::Tag;

    for (size_t index = 0; index < rowsRaw.size(); index++) {
        const CellView element = sourceList(lists, rowsRaw[index]).elements()[positionsRaw[index]];
        const CellTag tag = element.getTag();

        if (tag == CellTag::Null) {
            outputRaw[index] = IDType {};
            continue;
        }

        bioassert(tag == expectedTag, "Unwound element does not have the unwound list's entity type.");

        outputRaw[index] = element.getAs<IDType>();
    }
}

// The path sibling of unwindListValidIDEmit: a path column spells a null path as one holding
// no node, so the tagged null a list holds drains back into one.
template <typename SourceColumn>
void unwindListPathEmit(const Column* source,
                        const ColumnVector<size_t>* rows,
                        const ColumnVector<size_t>* positions,
                        Column* output) {
    const SourceColumn* lists = static_cast<const SourceColumn*>(source);
    const std::vector<size_t>& rowsRaw = rows->getRaw();
    const std::vector<size_t>& positionsRaw = positions->getRaw();

    std::vector<GraphPath>& outputRaw = static_cast<ColumnVector<GraphPath>*>(output)->getRaw();
    outputRaw.resize(rowsRaw.size());

    for (size_t index = 0; index < rowsRaw.size(); index++) {
        const CellView element = sourceList(lists, rowsRaw[index]).elements()[positionsRaw[index]];
        const CellTag tag = element.getTag();

        if (tag == CellTag::Null) {
            outputRaw[index].clear();
            continue;
        }

        bioassert(tag == CellTag::Path, "Unwound element of a list of paths is not a path.");

        outputRaw[index].assign(element.getAs<PathView>());
    }
}

size_t taggedCellRowCount(const CellView& element) {
    const std::optional<ListView> list = iteratedList(element);
    if (!list) {
        return 0;
    }

    return list->size();
}

CellView taggedCellElement(const CellView& element, size_t position) {
    return iteratedList(element)->elements()[position];
}

size_t unwindTaggedElementCount(const Column* source, size_t row) {
    const auto* elements = static_cast<const ColumnVector<CellView>*>(source);

    return taggedCellRowCount((*elements)[row]);
}

// Fill the element chunk from a type-erased column: a cell holding a nested list gives up
// the element at this row's position, and any other cell is itself the element.
void unwindTaggedElementEmit(const Column* source,
                             const ColumnVector<size_t>* rows,
                             const ColumnVector<size_t>* positions,
                             Column* output) {
    const std::vector<CellView>& elements = static_cast<const ColumnVector<CellView>*>(source)->getRaw();
    const std::vector<size_t>& rowsRaw = rows->getRaw();
    const std::vector<size_t>& positionsRaw = positions->getRaw();

    std::vector<CellView>& outputRaw = static_cast<ColumnVector<CellView>*>(output)->getRaw();
    outputRaw.resize(rowsRaw.size());

    for (size_t index = 0; index < rowsRaw.size(); index++) {
        outputRaw[index] = taggedCellElement(elements[rowsRaw[index]], positionsRaw[index]);
    }
}

// The value of T a search reads off @param bound when its loop starts: a T, or a cell of
// @param tag holding one. A null, and a cell of another kind, fail the search, naming
// @param operand and the @param type it must be.
template <typename T>
T readSearchedValue(const Column* bound, CellTag tag, std::string_view operand, std::string_view type) {
    std::optional<T> value;
    std::optional<CellView> cell;
    CellTag held = CellTag::Null;

    if (readBoundConstant(bound, cell)) {
        held = cell ? cell->getTag() : CellTag::Null;
        if (held == tag) {
            value = cell->getAs<T>();
        }
    } else {
        const bool readsAValue = readBoundConstant(bound, value);
        bioassert(readsAValue, "A bound {} holds one value or one cell for the run", operand);
    }

    if (!value) {
        throw IRException(fmt::format("VECTOR SEARCH {} must be {}, got {}", operand, type, queryTypeName(held)));
    }

    return *value;
}

std::string_view searchedIndexName(const NLVectorSearchLoopData& loopData) {
    const Column* bound = loopData.getBoundIndexName();
    if (!bound) {
        return loopData.getIndexName();
    }

    return readSearchedValue<types::String::Primitive>(bound, CellTag::String, "index name", "a String");
}

size_t searchedNeighbourCount(const NLVectorSearchLoopData& loopData) {
    const Column* bound = loopData.getBoundNeighbourCount();
    if (!bound) {
        return loopData.getNeighbourCount();
    }

    std::optional<CellView> cell;
    const bool holdsAnUnsignedCell = readBoundConstant(bound, cell) && cell && cell->getTag() == CellTag::UInt;
    if (holdsAnUnsignedCell) {
        const types::UInt64::Primitive count = cell->getAs<types::UInt64::Primitive>();
        if (count == 0) {
            throw IRException("VECTOR SEARCH k value must be greater than 0, got 0");
        }

        return static_cast<size_t>(count);
    }

    const types::Int64::Primitive count = readSearchedValue<types::Int64::Primitive>(bound, CellTag::Int, "k value", "an integer");
    if (count < 1) {
        throw IRException(fmt::format("VECTOR SEARCH k value must be greater than 0, got {}", count));
    }

    return static_cast<size_t>(count);
}

std::span<const float> searchedQueryVector(const NLVectorSearchLoopData& loopData) {
    const Column* bound = loopData.getBoundQueryVector();
    if (!bound) {
        return loopData.getQueryVector();
    }

    return readSearchedValue<types::Embedding::Primitive>(bound, CellTag::Embedding, "query vector", "an Embedding");
}

// The position of each field a CSV load produces, in field order: the one it named, or
// the one its header sits at in the file's header line. A position past the file's last
// field, or a header the file does not carry, is a query naming a field that is not
// there, so it is reported rather than left to read an absent column.
void resolveCSVFieldIndices(const NLLoadCSVLoopData& loopData,
                            const CSVFileInfo& fileInfo,
                            std::vector<size_t>& indices) {
    const std::vector<std::string>& headers = fileInfo._headers;

    indices.reserve(loopData.fields().size());

    for (const NLLoadCSVLoopData::Field& field : loopData.fields()) {
        if (field._byHeader) {
            const auto foundIt = std::ranges::find(headers, field._header);
            if (foundIt == end(headers)) {
                throw IRException(fmt::format("CSV header '{}' not found in '{}'",
                                              field._header,
                                              loopData.getPath()));
            }

            indices.push_back(static_cast<size_t>(std::distance(begin(headers), foundIt)));
        } else {
            if (field._index >= fileInfo._fieldCount) {
                throw IRException(fmt::format("CSV field {} is out of range: '{}' carries {} fields",
                                              field._index,
                                              loopData.getPath(),
                                              fileInfo._fieldCount));
            }

            indices.push_back(field._index);
        }
    }
}

// Every cell of the column is a list of its own, so none of them is the null a
// comprehension hands its row back.
bool presentCell(const Column*, size_t) {
    return false;
}

bool absentListCell(const Column* source, size_t row) {
    return !static_cast<const ColumnOptVector<ListView>*>(source)->isValid(row);
}

template <typename Primitive>
bool absentValueCell(const Column* source, size_t row) {
    return !static_cast<const ColumnOptVector<Primitive>*>(source)->isValid(row);
}

// A cell of a type-erased column carries its own tag, so the null is the one that tag
// names rather than a cell the column does not hold.
bool absentTaggedCell(const Column* source, size_t row) {
    const CellView& element = (*static_cast<const ColumnVector<CellView>*>(source))[row];

    return !iteratedList(element);
}

// Rebuild every column carried past a call from the input rows the procedure reported
// for the rows it just emitted: an input row it emitted several rows for is repeated,
// one it emitted none for is dropped - the gather an edge hop replicates its carry set
// with, over a row map a callback filled rather than a chunk writer.
//
// Throws when the procedure reported no row for every row it emitted, or one outside
// the chunk it was handed: either would leave the carried columns misaligned, silently
// pairing the wrong rows in the projection, so it is caught at the step that did it.
void gatherProcedureCarriedColumns(NLProcedureLoopData* loopData) {
    NLProcedureState* state = loopData->getState();
    ColumnIndices* indices = loopData->getIndices();

    const size_t emittedRows = state->getRowCount();
    const std::vector<size_t>& indicesRaw = indices->getRaw();
    if (indicesRaw.size() != emittedRows) {
        throw IRException(fmt::format("Procedure '{}' emitted {} rows but reported the input row of "
                                      "{} of them, so the columns carried past the call cannot be "
                                      "aligned with its result",
                                      state->getProcedure()->getFullName(),
                                      emittedRows,
                                      indicesRaw.size()));
    }

    // Each index selects the input row a carried value is replicated from, so one out
    // of range would read past the chunk the procedure was handed.
    const size_t inputRows = state->getInputRowCount();
    for (const size_t inputRow : indicesRaw) {
        if (inputRow >= inputRows) {
            throw IRException(fmt::format("Procedure '{}' reported input row {} for a chunk of {} "
                                          "rows",
                                          state->getProcedure()->getFullName(),
                                          inputRow,
                                          inputRows));
        }
    }

    for (const NLCarriedColumn& carriedColumn : loopData->carriedColumns()) {
        const NLGatherFunction gather = carriedColumn.getGatherFunc();
        gather(carriedColumn.getInput(), indices, carriedColumn.getOutput());
    }
}

// Drive a procedure through one loop: each step runs its execute callback once through
// runStep, refilling the loop variables in place, then rebuilds any carried column and
// runs the body. The loop ends when the procedure declares itself finished, so one entry
// may cover as many chunks as it needs.
//
// The body sees only the steps that produced rows. A procedure declares itself finished
// on the step that exhausts it - which may still carry rows - so the flag is read after
// the call, not tested before it.
template <typename StepFunction>
void runProcedureDrive(NLExecutionContext* context,
                       NLProcedureLoopData* loopData,
                       StepFunction runStep) {
    NLProcedureState* state = loopData->getState();
    const NLStmtContainer* loopBody = loopData->getStmts();
    const NLProcedureLoopData::CarriedColumns& carriedColumns = loopData->carriedColumns();

    // A null limit leaves the drive unbounded; otherwise it stops once the budget is
    // spent, so a LIMIT ends the drive rather than running the procedure out. The limit
    // is fixed for the whole loop, so the null check is hoisted out of the per-iteration
    // condition, as the scan loops do.
    const NLLimitState* limit = loopData->getLimit();

    bool finished = false;
    const auto runIteration = [&]() {
        // The procedure reports the input row behind each row it emits, so clear the
        // map before the call: what it appends is this step's mapping alone, as its
        // result columns are.
        if (!carriedColumns.empty()) {
            loopData->getIndices()->clear();
        }

        runStep();
        finished = state->isFinished();

        if (state->getRowCount() == 0) {
            return;
        }

        if (!carriedColumns.empty()) {
            gatherProcedureCarriedColumns(loopData);
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (!finished && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (!finished) {
            runIteration();
        }
    }
}

}

void NLExecutor::runUnwindConstLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLUnwindConstLoopData* loopData = static_cast<NLUnwindConstLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    const ListView list = loopData->getList();
    Column* output = loopData->getOutput();
    const bool heterogeneous = loopData->isHeterogeneous();
    const ValueType valueType = loopData->getValueType();
    const size_t chunkSize = context->getChunkSize();
    const size_t totalCount = list.size();

    // A null limit leaves the loop unbounded, exactly as in runConstScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    // Emit the fixed list one chunk at a time: each step fills the value chunk with the
    // next slice and runs the body over it. The cursor is local to this call, so an
    // unwind nested in a cross product restarts from the first element on every outer
    // step - the same way runConstScanNodesLoop reopens its slice each call.
    size_t cursor = 0;

    const auto runIteration = [&]() {
        const size_t remaining = totalCount - cursor;
        const size_t rows = std::min(chunkSize, remaining);

        if (heterogeneous) {
            fillListElementChunk(output, list, cursor, rows);
        } else {
            fillHomogeneousChunk(output, valueType, list, cursor, rows);
        }

        cursor += rows;

        runBody(context, loopBody);
    };

    if (limit) {
        while (cursor < totalCount && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (cursor < totalCount) {
            runIteration();
        }
    }
}

void NLExecutor::runLoadCSVLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLLoadCSVLoopData* loopData = static_cast<NLLoadCSVLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnStringTable* row = loopData->getRow();
    StringBuffer* const stringBuffer = loopData->getStringBuffer();
    const size_t chunkSize = context->getChunkSize();

    // A null limit leaves the loop unbounded, exactly as in runConstScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    const fs::Path* const dataDir = context->getDataDir();
    if (!dataDir) {
        throw IRException("Reading a CSV file needs a data directory, which this session has not opened");
    }

    fs::Path path;
    NLSystemContext::resolveInDataDir(path, *dataDir, loopData->getPath());

    // The file's shape decides what the fields the query named resolve to: a header line
    // names them, and the first record fixes how many a record carries - which is also
    // the count a malformed record is caught against.
    CSVFileInfo fileInfo;
    CSVParser::peekFileStructure(path, loopData->hasHeaders(), fileInfo);

    // A file holding no record names no field, so there is nothing to resolve a position
    // or a header against - and no row to run the body over either
    if (fileInfo._fieldCount == 0) {
        return;
    }

    std::vector<size_t> fieldIndices;
    resolveCSVFieldIndices(*loopData, fileInfo, fieldIndices);

    const CSVErrorMode errorMode = loopData->skipOnError() ? CSVErrorMode::Skip : CSVErrorMode::Fail;
    CSVParser parser(path, loopData->hasHeaders(), errorMode, fileInfo._fieldCount);

    // Parse the file one chunk of records at a time: each step fills the field columns
    // with the next records and runs the body over them. The parser is local to this
    // call, so a load nested in a cross product reopens the file on every outer step -
    // the same way runScanNodesLoop opens a fresh chunk writer each call.
    bool exhausted = false;

    const auto runIteration = [&]() {
        stringBuffer->clear();
        const size_t rows = parser.readChunk(chunkSize, fieldIndices, row, stringBuffer);

        if (rows == 0) {
            exhausted = true;
            return;
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (!exhausted && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (!exhausted) {
            runIteration();
        }
    }
}

void NLExecutor::searchVectorIndex(NLExecutionContext* context, NLVectorSearchLoopData* loopData) {
    const std::string_view indexName = searchedIndexName(*loopData);
    const size_t neighbourCount = searchedNeighbourCount(*loopData);
    const std::span<const float> queryVector = searchedQueryVector(*loopData);

    if (loopData->hasSearched(indexName, neighbourCount, queryVector)) {
        return;
    }

    vec::VectorDatabase* const vectorDatabase = context->getVectorDatabase();
    if (!vectorDatabase) {
        throw IRException("A vector search needs a vector database, which this session has not opened");
    }

    vec::VecLibAccessor accessor = vectorDatabase->getLibrary(indexName);
    if (!accessor.isValid()) {
        throw IRException(fmt::format("Vector index '{}' not found", indexName));
    }

    // The index reads the query vector as a flat span of exactly its dimension, so a
    // shorter one would be read past its end rather than rejected further down.
    const vec::Dimension dimension = accessor.metadata()->_dimension;
    if (queryVector.size() != dimension) {
        throw IRException(fmt::format("Vector index '{}' holds vectors of dimension {}, but the "
                                      "query searches for one of dimension {}",
                                      indexName,
                                      dimension,
                                      queryVector.size()));
    }

    vec::VectorSearchQuery query(dimension);
    query.setVector(queryVector);
    query.setMaxResultCount(neighbourCount);

    vec::VectorSearchResult result;
    const vec::VectorResult<void> searched = accessor.search(&query, &result);
    if (!searched.has_value()) {
        throw IRException(fmt::format("Vector search in '{}' failed: {}",
                                      indexName,
                                      searched.error().fmtMessage()));
    }

    // The neighbours come back nearest first, the two spans row-aligned, so either one
    // measures the result.
    const std::span<const int64_t> ids = result.ids();
    const std::span<const float> distances = result.distances();

    // An ID naming no node is kept rather than dropped the way a const scan drops one: it
    // matches no edge and holds no property, so an index whose IDs are not node IDs still
    // reports every neighbour it found.
    std::vector<NodeID>& neighbourIDs = loopData->neighbourIDs();
    neighbourIDs.resize(ids.size());
    std::transform(ids.begin(),
                   ids.end(),
                   neighbourIDs.begin(),
                   [](int64_t id) { return NodeID {static_cast<uint64_t>(id)}; });

    std::vector<types::Double::Primitive>& neighbourScores = loopData->neighbourScores();
    neighbourScores.resize(distances.size());
    std::copy(distances.begin(), distances.end(), neighbourScores.begin());

    loopData->markSearched(indexName, neighbourCount, queryVector);
}

void NLExecutor::runVectorSearchLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLVectorSearchLoopData* loopData = static_cast<NLVectorSearchLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();

    // A null limit leaves the loop unbounded, exactly as in runUnwindConstLoop. A spent
    // one emits nothing, so the neighbours it would report are never read: the index is
    // asked for them only once the budget can take a row.
    const NLLimitState* limit = loopData->getLimit();
    if (limit && limit->getRemaining() == 0) {
        return;
    }

    // The reader lock the accessor holds lives no longer than the search itself, so a
    // concurrent LOAD VECTOR into this index waits for the search rather than for the
    // whole query the neighbours feed.
    searchVectorIndex(context, loopData);

    const std::span<const NodeID> ids = loopData->neighbourIDs();
    const std::span<const types::Double::Primitive> scores = loopData->neighbourScores();

    using ScoreColumn = ColumnOptVector<types::Double::Primitive>;

    ColumnNodeIDs* const idColumn = loopData->getIDs();
    ScoreColumn* const scoreColumn = static_cast<ScoreColumn*>(loopData->getScores());

    const size_t chunkSize = context->getChunkSize();
    const size_t totalCount = ids.size();

    // Emit the neighbours one chunk at a time: each step fills the two chunks with the
    // next slice and runs the body over them. The cursor is local to this call, so a
    // search nested in a cross product walks the neighbours again on every outer step -
    // the way runScanNodesLoop reopens its chunk writer each call.
    size_t cursor = 0;

    const auto runIteration = [&]() {
        const size_t remaining = totalCount - cursor;
        const size_t rows = std::min(chunkSize, remaining);

        std::vector<NodeID>& rawIDs = idColumn->getRaw();
        rawIDs.resize(rows);
        std::copy(ids.begin() + cursor, ids.begin() + cursor + rows, rawIDs.begin());

        resetForRowWrites(scoreColumn, rows);
        std::copy(scores.begin() + cursor, scores.begin() + cursor + rows, scoreColumn->getValues().begin());

        cursor += rows;

        runBody(context, loopBody);
    };

    if (limit) {
        while (cursor < totalCount && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (cursor < totalCount) {
            runIteration();
        }
    }
}

void NLExecutor::runUnwindLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLUnwindLoopData* loopData = static_cast<NLUnwindLoopData*>(data);
    const Column* source = loopData->getSource();

    const NLStmtContainer* loopBody = loopData->getStmts();
    const NLUnwindElementEmitFunction elementEmit = loopData->getElementEmitFunc();
    const size_t chunkSize = context->getChunkSize();

    // A null limit leaves the loop unbounded, exactly as in runUnwindConstLoop.
    const NLLimitState* limit = loopData->getLimit();

    ColumnVector<size_t>* rows = loopData->getRows();
    ColumnVector<size_t>* positions = loopData->getPositions();

    NLElementCursor cursor(source, loopData->getElementCountFunc());

    const auto runIteration = [&]() {
        cursor.fillChunk(chunkSize, rows->getRaw(), elementEmit ? &positions->getRaw() : nullptr);

        // A source whose cells hold more than the element drains through its own emit;
        // any other holds the elements already and rides the carry set, gathered by the
        // source row like everything else in flight.
        if (elementEmit) {
            elementEmit(source, rows, positions, loopData->getElementOutput());
        }

        for (const NLCarriedColumn& carriedColumn : loopData->carriedColumns()) {
            const auto gatherFunc = carriedColumn.getGatherFunc();
            gatherFunc(carriedColumn.getInput(), rows, carriedColumn.getOutput());
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (!cursor.exhausted() && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (!cursor.exhausted()) {
            runIteration();
        }
    }
}

void NLExecutor::runUnwindRangeLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLUnwindRangeLoopData* loopData = static_cast<NLUnwindRangeLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    const size_t chunkSize = context->getChunkSize();

    // A null limit leaves the loop unbounded, exactly as in runUnwindLoop.
    const NLLimitState* limit = loopData->getLimit();

    using ElementColumn = ColumnOptVector<types::Int64::Primitive>;
    ElementColumn* const elements = static_cast<ElementColumn*>(loopData->getElementOutput());
    ColumnVector<size_t>* rows = loopData->getRows();

    NLRangeCursor cursor(loopData);

    const auto runIteration = [&]() {
        cursor.fillChunk(chunkSize, elements, rows->getRaw());

        for (const NLCarriedColumn& carriedColumn : loopData->carriedColumns()) {
            const auto gatherFunc = carriedColumn.getGatherFunc();
            gatherFunc(carriedColumn.getInput(), rows, carriedColumn.getOutput());
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (!cursor.exhausted() && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (!cursor.exhausted()) {
            runIteration();
        }
    }
}

void NLExecutor::runUnwindComputedLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLUnwindComputedLoopData* loopData = static_cast<NLUnwindComputedLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    const NLStmtContainer* sourceStmts = loopData->getSourceStmts();
    const size_t chunkSize = context->getChunkSize();

    // A null limit leaves the loop unbounded, exactly as in runUnwindLoop.
    const NLLimitState* limit = loopData->getLimit();
    const auto withinLimit = [limit]() {
        return !limit || limit->getRemaining() > 0;
    };

    const NLUnwindElementCountFunction elementCount = loopData->getElementCountFunc();
    const NLUnwindElementEmitFunction elementEmit = loopData->getElementEmitFunc();
    const NLCarriedColumn* elementCell = loopData->getElementCell();
    const Column* source = loopData->getSource();

    ColumnVector<size_t>* runRows = loopData->getRunRows();
    ColumnVector<size_t>* rows = loopData->getRows();
    ColumnVector<size_t>* positions = loopData->getPositions();
    std::vector<size_t>& runRaw = runRows->getRaw();
    std::vector<size_t>& rowsRaw = rows->getRaw();

    const size_t inputRows = loopData->getInputRows();

    // A run of input rows holds about one chunk of elements once the first run has shown
    // how many a row spans, so the source never holds the elements of a whole chunk of rows
    size_t runStart = 0;
    size_t runLength = 1;

    while (runStart < inputRows && withinLimit()) {
        runLength = std::min(runLength, inputRows - runStart);

        runRaw.resize(runLength);
        std::iota(runRaw.begin(), runRaw.end(), runStart);

        for (const NLCarriedColumn& argument : loopData->sourceArguments()) {
            const auto gatherFunc = argument.getGatherFunc();
            gatherFunc(argument.getInput(), runRows, argument.getOutput());
        }

        runBody(context, sourceStmts);

        NLElementCursor cursor(source, elementCount);
        size_t elements = 0;

        while (!cursor.exhausted() && withinLimit()) {
            cursor.fillChunk(chunkSize, rowsRaw, elementEmit ? &positions->getRaw() : nullptr);
            elements += rowsRaw.size();

            if (elementEmit) {
                elementEmit(source, rows, positions, loopData->getElementOutput());
            } else if (elementCell) {
                const auto gatherFunc = elementCell->getGatherFunc();
                gatherFunc(elementCell->getInput(), rows, elementCell->getOutput());
            }

            std::ranges::transform(rowsRaw, rowsRaw.begin(), [runStart](size_t row) {
                return row + runStart;
            });

            for (const NLCarriedColumn& carriedColumn : loopData->carriedColumns()) {
                const auto gatherFunc = carriedColumn.getGatherFunc();
                gatherFunc(carriedColumn.getInput(), rows, carriedColumn.getOutput());
            }

            runBody(context, loopBody);
        }

        runStart += runLength;
        runLength = elements == 0 ? chunkSize : std::max<size_t>(1, chunkSize * runLength / elements);
    }
}

NLCellAbsentFunction NLExecutor::selectPresentCell() {
    return &presentCell;
}

NLCellAbsentFunction NLExecutor::selectListCellAbsent() {
    return &absentListCell;
}

NLCellAbsentFunction NLExecutor::selectTaggedCellAbsent() {
    return &absentTaggedCell;
}

NLCellAbsentFunction NLExecutor::selectValueCellAbsent(NLChunkKind kind) {
    NLCellAbsentFunction selected = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { selected = &absentValueCell<Element>; });

    return selected;
}

NLUnwindElementCountFunction NLExecutor::selectListUnwindElementCount(bool sourceIsNullable) {
    return sourceIsNullable ? &unwindOptListElementCount : &unwindListElementCount;
}

NLUnwindElementCountFunction NLExecutor::selectOptUnwindElementCount(NLChunkKind kind) {
    NLUnwindElementCountFunction selected = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { selected = &unwindOptElementCount<Element>; });

    return selected;
}

NLUnwindElementCountFunction NLExecutor::selectValueUnwindElementCount() {
    return &unwindValueElementCount;
}

NLUnwindElementEmitFunction NLExecutor::selectListUnwindElementEmit(bool sourceIsNullable) {
    return sourceIsNullable ? &unwindListElementEmit<ColumnOptVector<ListView>>
                            : &unwindListElementEmit<ColumnVector<ListView>>;
}

NLUnwindElementEmitFunction NLExecutor::selectListUnwindValueEmit(bool sourceIsNullable, NLChunkKind kind) {
    NLUnwindElementEmitFunction selected = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() {
        selected = sourceIsNullable ? &unwindListValueEmit<ColumnOptVector<ListView>, Element>
                                    : &unwindListValueEmit<ColumnVector<ListView>, Element>;
    });

    return selected;
}

NLUnwindElementEmitFunction NLExecutor::selectListUnwindNodeEmit(bool sourceIsNullable) {
    return sourceIsNullable ? &unwindListValidIDEmit<ColumnOptVector<ListView>, NodeID>
                            : &unwindListValidIDEmit<ColumnVector<ListView>, NodeID>;
}

NLUnwindElementEmitFunction NLExecutor::selectListUnwindEdgeEmit(bool sourceIsNullable) {
    return sourceIsNullable ? &unwindListValidIDEmit<ColumnOptVector<ListView>, EdgeID>
                            : &unwindListValidIDEmit<ColumnVector<ListView>, EdgeID>;
}

NLUnwindElementEmitFunction NLExecutor::selectListUnwindPathEmit(bool sourceIsNullable) {
    return sourceIsNullable ? &unwindListPathEmit<ColumnOptVector<ListView>>
                            : &unwindListPathEmit<ColumnVector<ListView>>;
}

NLUnwindElementCountFunction NLExecutor::selectTaggedUnwindElementCount() {
    return &unwindTaggedElementCount;
}

NLUnwindElementEmitFunction NLExecutor::selectTaggedUnwindElementEmit() {
    return &unwindTaggedElementEmit;
}

void NLExecutor::runProcedureInitLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLProcedureLoopData* loopData = static_cast<NLProcedureLoopData*>(data);
    NLProcedureState* state = loopData->getState();

    state->setExecutionContext(context);

    // This loop drives the procedure over one chunk of its arguments and is re-entered
    // for the next chunk, so rewind it here: a procedure that finished the previous
    // chunk starts afresh on this one, and its own per-drive state goes with it.
    state->prepareOrResetForNewDrive();

    runProcedureDrive(context, loopData, [state]() { state->execute(); });
}
