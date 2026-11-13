#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>
#include <bit>
#include <cmath>
#include <concepts>
#include <functional>
#include <limits>
#include <numeric>
#include <queue>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

#include <spdlog/fmt/bundled/format.h>
#include <llvm/Support/MathExtras.h>

#include "NumberOrder.h"
#include "TypeUtils.h"
#include "columns/ContainerKind.h"
#include "iterators/GetEdgesIterator.h"
#include "ID.h"
#include "iterators/GetEdgeTypesIterator.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetInEdgesByTypeIterator.h"
#include "iterators/GetInEdgesByLabelIterator.h"
#include "iterators/GetInEdgesByTypeAndLabelIterator.h"
#include "iterators/GetNodeLabelSetIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetOutEdgesByTypeIterator.h"
#include "iterators/GetOutEdgesByLabelIterator.h"
#include "iterators/GetOutEdgesByTypeAndLabelIterator.h"
#include "iterators/GetPropertiesWithNullIterator.h"
#include "iterators/PathDistanceIndex.h"
#include "iterators/PathExplorator.h"
#include "iterators/PathTargetIndex.h"
#include "iterators/PathHopFilter.h"
#include "iterators/PathLabelHopFilter.h"
#include "iterators/ReversedPathExplorator.h"
#include "iterators/ScanEdgesByEndpointLabelsChunkWriter.h"
#include "iterators/ScanEdgesByTypeIterator.h"
#include "iterators/ScanEdgesIterator.h"
#include "iterators/ScanInEdgesByTargetLabelIterator.h"
#include "iterators/ScanOutEdgesBySourceLabelIterator.h"
#include "iterators/ScanNodesIterator.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnCombinations.h"
#include "columns/ColumnOperator.h"
#include "columns/ColumnOperators.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/AllowedKinds.h"
#include "columns/Functions.h"
#include "columns/IndexTypeError.h"
#include "columns/UnaryPredicates.h"
#include "CellOrder.h"
#include "CellUtils.h"
#include "IteratedList.h"
#include "QueryTypeName.h"
#include "list/ListHash.h"
#include "map/MapHash.h"
#include "map/MapUtils.h"
#include "list/PathTrie.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

#include "reader/GraphReader.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/PendingAdjacency.h"
#include "versioning/ScannedNodes.h"
#include "views/GraphView.h"

#include "CSVParser.h"

#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "VecLibAccessor.h"
#include "VectorDatabase.h"
#include "VectorSearchQuery.h"
#include "VectorSearchResult.h"

#include "NLSystemContext.h"

#include "NLKeyHash.h"
#include "NLSerializedKey.h"
#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLMergeExecutor.h"
#include "NLPendingEdges.h"
#include "NLPendingNodes.h"
#include "NLPropertyValueScan.h"
#include "NLEntityProperties.h"
#include "NLWriteProperties.h"
#include "NLWrittenValues.h"
#include "NLPropertyFunctions.h"
#include "NLMergeWorkingSet.h"
#include "NLOutputSink.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "IRRange.h"
#include "BioAssert.h"
#include "Overloaded.h"

using namespace db;

namespace {

// The step row an untagged mark, tally or collect answers for: the position its row chunk
// holds, or the single row of the step when it has none
uint64_t untaggedRow(const ColumnVector<uint64_t>* row) {
    return row ? row->getRaw().front() : 0;
}

// The cursor an unwind loop and a list comprehension both walk: every (row, element) pair
// of a source column in row order. A cell contributing nothing - a null, an empty list -
// is skipped, so it emits no row. The cursor is local to the walk, so one nested in an
// outer loop restarts on every one of its steps.
class NLElementCursor {
public:
    NLElementCursor(const Column* source, NLUnwindElementCountFunction elementCount)
        : _source(source),
        _elementCount(elementCount),
        _sourceRows(source->size())
    {
        openNextRow();
    }

    bool exhausted() const { return _sourceRow >= _sourceRows; }
    size_t getRow() const { return _sourceRow; }
    size_t getSourceRows() const { return _sourceRows; }

    // Takes the next @param chunkSize pairs, or what is left of them: the source row each
    // pair came from, and - for a source whose cells hold more than the element - which
    // element of its cell the pair took, which the emit handler reads.
    void fillChunk(size_t chunkSize, std::vector<size_t>& rows, std::vector<size_t>* positions) {
        rows.clear();
        if (positions) {
            positions->clear();
        }

        while (rows.size() < chunkSize && !exhausted()) {
            rows.push_back(_sourceRow);

            if (positions) {
                positions->push_back(_elementIndex);
            }

            _elementIndex++;

            if (_elementIndex == _rowElements) {
                skipRow();
            }
        }
    }

    void skipRow() {
        _elementIndex = 0;
        _sourceRow++;
        openNextRow();
    }

private:
    const Column* _source {nullptr};
    NLUnwindElementCountFunction _elementCount {nullptr};
    size_t _sourceRows {0};
    size_t _sourceRow {0};
    size_t _elementIndex {0};
    size_t _rowElements {0};

    void openNextRow() {
        while (_sourceRow < _sourceRows) {
            _rowElements = _elementCount(_source, _sourceRow);
            if (_rowElements > 0) {
                return;
            }

            _sourceRow++;
        }
    }
};

// The integers one row's range bounds span. A row with a null bound spans no list at all,
// and one whose step walks away from its end spans the empty list.
class NLRangeSpan {
public:
    // Returns false where a bound of @param row holds no value
    bool read(const NLRangeBound& start, const NLRangeBound& end, const NLRangeBound& step, size_t row) {
        const std::optional<types::Int64::Primitive> from = start._read(start._column, row);
        const std::optional<types::Int64::Primitive> to = end._read(end._column, row);
        const std::optional<types::Int64::Primitive> by =
            step._column ? step._read(step._column, row) : std::optional<types::Int64::Primitive> {1};

        const bool boundsPresent = from.has_value() && to.has_value() && by.has_value();
        if (!boundsPresent) {
            return false;
        }

        if (*by == 0) {
            throw IRException("range() cannot count by a step of 0");
        }

        _windowFirst = 0;
        _reversed = false;
        _ascending = *by > 0;
        _empty = _ascending ? *from > *to : *from < *to;
        _first = static_cast<uint64_t>(*from);
        _stride = _ascending ? static_cast<uint64_t>(*by) : 0 - static_cast<uint64_t>(*by);
        _steps = countRangeSteps(*from, *to, *by);

        return true;
    }

    // Narrows the span to the run a slice of its list keeps, as runListSlice cuts it, and
    // counts that run out backwards where @param window is reversed. Returns false where a
    // slice bound of @param row holds no value.
    bool keepWindow(const NLRangeWindowBounds& window, size_t row) {
        const NLRangeBound& from = window._from;
        const NLRangeBound& to = window._to;

        const std::optional<types::Int64::Primitive> lower =
            from._column ? from._read(from._column, row) : std::optional<types::Int64::Primitive> {0};
        const std::optional<types::Int64::Primitive> upper =
            to._column ? to._read(to._column, row) : std::optional<types::Int64::Primitive> {};

        const bool boundsPresent = lower.has_value() && (!to._column || upper.has_value());
        if (!boundsPresent) {
            return false;
        }

        _reversed = window._reversed;

        if (_empty) {
            return true;
        }

        // The list holds _steps + 1 integers, up to 2^64, so the run is located by its first
        // and last positions, and a bound counting from the end by how far it stops short of it
        uint64_t first = 0;
        if (*lower >= 0) {
            first = static_cast<uint64_t>(*lower);
        } else {
            const uint64_t shortOfLast = static_cast<uint64_t>(-(*lower + 1));
            first = shortOfLast >= _steps ? 0 : _steps - shortOfLast;
        }

        const types::Int64::Primitive upperBound = upper.value_or(0);

        uint64_t last = 0;
        bool upperKeepsAny = true;
        if (!to._column) {
            last = _steps;
        } else if (upperBound > 0) {
            last = std::min(static_cast<uint64_t>(upperBound) - 1, _steps);
        } else if (upperBound == 0) {
            upperKeepsAny = false;
        } else {
            const uint64_t shortOfLast = static_cast<uint64_t>(-(upperBound + 1));
            upperKeepsAny = shortOfLast < _steps;
            last = upperKeepsAny ? _steps - shortOfLast - 1 : 0;
        }

        _empty = !upperKeepsAny || first > last;
        _windowFirst = first;
        _steps = _empty ? 0 : last - first;

        return true;
    }

    bool isEmpty() const { return _empty; }
    uint64_t getSteps() const { return _steps; }

    // The stride can be wider than an int64 holds, so every integer is offset from the
    // first unsigned
    types::Int64::Primitive at(uint64_t position) const {
        const uint64_t listPosition = _reversed ? _windowFirst + _steps - position : _windowFirst + position;
        const uint64_t offset = listPosition * _stride;
        return static_cast<types::Int64::Primitive>(_ascending ? _first + offset : _first - offset);
    }

private:
    uint64_t _first {0};
    uint64_t _stride {0};
    uint64_t _steps {0};
    uint64_t _windowFirst {0};
    bool _ascending {true};
    bool _empty {true};
    bool _reversed {false};
};

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
    void fillChunk(size_t chunkSize, std::vector<std::optional<types::Int64::Primitive>>* values, std::vector<size_t>& rows) {
        rows.resize(chunkSize);

        size_t filled = 0;
        while (filled < chunkSize && !exhausted()) {
            const size_t room = chunkSize - filled;
            const uint64_t remainingSteps = _span.getSteps() - _position;
            const bool finishesRow = remainingSteps < room;
            const size_t count = finishesRow ? static_cast<size_t>(remainingSteps) + 1 : room;

            if (values) {
                values->resize(filled + count);

                for (size_t index = 0; index < count; index++) {
                    (*values)[filled + index] = _span.at(_position + index);
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

        std::vector<std::optional<Primitive>>& raw = typed->getRaw();
        raw.resize(rows);

        // CellView::getAs reinterprets the element's bytes as the primitive
        // without consulting its type tag, so check the tag against the one the
        // primitive is stored under: a homogeneous unwind whose column type disagrees
        // with its literals would otherwise read a value of the wrong type.
        constexpr CellTag expectedTag = TypeToCellTag<Primitive>::Tag;

        for (size_t index = 0; index < rows; index++) {
            const CellView element = elements[offset + index];
            const CellTag tag = element.getTag();

            if (tag == CellTag::Null) {
                raw[index] = std::nullopt;
                continue;
            }

            bioassert(tag == expectedTag, "Unwound element does not have the unwind's value type.");

            raw[index] = element.getAs<Primitive>();
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
    return *source->getRaw()[row];
}

// The rows one cell of a list column unwinds into: one per element, so an empty list
// contributes none.
size_t unwindListElementCount(const Column* source, size_t row) {
    const auto* lists = static_cast<const ColumnVector<ListView>*>(source);
    return (*lists)[row].size();
}

// The nullable sibling: an absent list unwinds into no row at all, as a null value does.
size_t unwindOptListElementCount(const Column* source, size_t row) {
    const std::optional<ListView>& list = static_cast<const ColumnOptVector<ListView>*>(source)->getRaw()[row];
    return list.has_value() ? list->size() : 0;
}

// The rows one cell of a nullable value column unwinds into: the single row a present
// value is, and none for a null - Cypher's UNWIND of a null.
template <typename Primitive>
size_t unwindOptElementCount(const Column* source, size_t row) {
    const auto* values = static_cast<const ColumnOptVector<Primitive>*>(source);
    return (*values)[row].has_value() ? 1 : 0;
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

    std::vector<std::optional<Primitive>>& outputRaw = static_cast<ColumnOptVector<Primitive>*>(output)->getRaw();
    outputRaw.resize(rowsRaw.size());

    constexpr CellTag expectedTag = TypeToCellTag<Primitive>::Tag;

    for (size_t index = 0; index < rowsRaw.size(); index++) {
        const CellView element = sourceList(lists, rowsRaw[index]).elements()[positionsRaw[index]];
        const CellTag tag = element.getTag();

        if (tag == CellTag::Null) {
            outputRaw[index] = std::nullopt;
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

template <typename Functor>
Functor makeFunctor(NLExecutionContext* context, NLStepBuffers* memory) {
    if constexpr (std::is_constructible_v<Functor, NLExecutionContext*, NLStepBuffers*>) {
        return Functor(context, memory);
    } else if constexpr (std::is_constructible_v<Functor, GraphView, QueryListBuffer*, const CommitWriteBuffer*>) {
        return Functor(*context->getView(), &memory->listBuffer(), context->getWriteBuffer());
    } else if constexpr (std::is_constructible_v<Functor, GraphView, StringBuffer*, const CommitWriteBuffer*>) {
        return Functor(*context->getView(), &memory->stringBuffer(), context->getWriteBuffer());
    } else if constexpr (std::is_constructible_v<Functor, GraphView, const CommitWriteBuffer*>) {
        return Functor(*context->getView(), context->getWriteBuffer());
    } else if constexpr (std::is_constructible_v<Functor, GraphView>) {
        return Functor(*context->getView());
    } else if constexpr (std::is_constructible_v<Functor, QueryListBuffer*, StringBuffer*>) {
        return Functor(&memory->listBuffer(), &memory->stringBuffer());
    } else if constexpr (std::is_constructible_v<Functor, QueryListBuffer*>) {
        return Functor(&memory->listBuffer());
    } else if constexpr (std::is_constructible_v<Functor, StringBuffer*>) {
        return Functor(&memory->stringBuffer());
    } else {
        return Functor {};
    }
}

template <typename Functor>
void functionConstKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    auto* output = static_cast<ColumnConst<Res>*>(result);
    Functor functor = makeFunctor<Functor>(context, memory);

    if (const auto* typedInput = dynamic_cast<const ColumnConst<Arg>*>(input)) {
        output->set(functor(typedInput->getRaw()));
        return;
    }

    bioassert(false, "Function operand has an unexpected column type.");
}

// The constant whose single cell can be absent: the functor is handed the optional and
// answers the absence itself, as functionNullReadingKernel hands it one per row.
template <typename Functor>
void functionOptConstKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    const auto* typedInput = dynamic_cast<const ColumnConst<std::optional<Arg>>*>(input);
    bioassert(typedInput, "Function operand has an unexpected column type.");
    auto* output = static_cast<ColumnConst<Res>*>(result);

    Functor functor = makeFunctor<Functor>(context, memory);
    output->set(functor(typedInput->getRaw()));
}

template <typename Functor, typename Element>
void applyFunctionOverNullableConst(Functor& functor,
                                    const ColumnConst<std::optional<Element>>* input,
                                    ColumnConst<std::optional<TypeUtils::unwrap_optional_t<typename Functor::ResultType>>>* output) {
    const std::optional<Element>& value = input->getRaw();
    if (!value.has_value()) {
        output->set(std::nullopt);
        return;
    }

    output->set(functor(*value));
}

// The same constant for a function that does not read its own nulls: an absent cell
// answers null, as functionOptKernel answers one per row.
template <typename Functor>
void functionNullableConstKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using JustRes = TypeUtils::unwrap_optional_t<typename Functor::ResultType>;

    auto* output = static_cast<ColumnConst<std::optional<JustRes>>*>(result);
    Functor functor = makeFunctor<Functor>(context, memory);

    if (const auto* typedInput = dynamic_cast<const ColumnConst<std::optional<Arg>>*>(input)) {
        applyFunctionOverNullableConst(functor, typedInput, output);
        return;
    }

    bioassert(false, "Function operand has an unexpected column type.");
}

template <typename Arg>
bool isNullableConstantOf(const Column* input) {
    return input->getKind() == ColumnConst<std::optional<Arg>>::staticKind();
}

// A null constant argument converts to a null result whatever the function; the
// ColumnConst<PropertyNull> result already reads as null, so nothing is computed.
void functionNullKernel(NLExecutionContext*, Column*, const Column*, NLStepBuffers*) {
}

template <typename Functor, typename Element>
void applyFunctionOverVector(Functor& functor,
                             const ColumnVector<Element>* input,
                             ColumnVector<typename Functor::ResultType>* output) {
    const auto& inputRaw = input->getRaw();
    const size_t size = inputRaw.size();

    output->resize(size);
    auto& outputRaw = output->getRaw();

    for (size_t row = 0; row < size; row++) {
        outputRaw[row] = functor(inputRaw[row]);
    }
}

// Whether a column holds one type-erased cell per row, which a list function reads
// through its tagged counterpart rather than as the list column it was written against.
bool readsTaggedCells(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    return kind == ColumnVector<CellView>::staticKind() || kind == ColumnConst<CellView>::staticKind();
}

template <typename Functor>
void functionVectorKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    auto* output = static_cast<ColumnVector<Res>*>(result);
    Functor functor = makeFunctor<Functor>(context, memory);

    if (const auto* typedInput = dynamic_cast<const ColumnVector<Arg>*>(input)) {
        applyFunctionOverVector(functor, typedInput, output);
        return;
    }

    bioassert(false, "Function operand has an unexpected column type.");
}

// The nullable-input kernel of a function that reads its own nulls: the absent value goes
// to the functor as it is, so the result rides the plain column its answers always fill.
template <typename Functor>
void functionNullReadingKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    const auto* typedInput = dynamic_cast<const ColumnOptVector<Arg>*>(input);
    bioassert(typedInput, "Function operand has an unexpected column type.");
    auto* output = static_cast<ColumnVector<Res>*>(result);

    const auto& inputRaw = typedInput->getRaw();
    const size_t size = inputRaw.size();

    output->resize(size);
    auto& outputRaw = output->getRaw();

    Functor functor = makeFunctor<Functor>(context, memory);
    for (size_t row = 0; row < size; row++) {
        outputRaw[row] = functor(inputRaw[row]);
    }
}

// The kernel serving a column of type-erased cells: a cell per row, or the single cell a
// constant is. The functor reads the null a cell's own tag carries, so its answer fills a
// plain column either way.
template <typename Functor>
NLUnaryFunctionKernel selectTaggedCellFunction(const Column* input, LocalMemory* memory, Column*& result) {
    using Res = typename Functor::ResultType;

    if (input->getContainerKind() == ContainerKind::code<ColumnConst>()) {
        result = memory->alloc<ColumnConst<Res>>();
        return &functionConstKernel<Functor>;
    }

    result = memory->alloc<ColumnVector<Res>>();
    return &functionVectorKernel<Functor>;
}

// A node or edge an OPTIONAL MATCH did not match is an invalid ID, so a function reading
// one produces a null rather than reading the graph at an ID that is not in it. The
// entity sibling of functionOptKernel: the input is a plain ID column, which carries its
// null in the ID itself instead of in an optional.
template <typename Functor>
void functionEntityKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;
    using JustRes = TypeUtils::unwrap_optional_t<Res>;

    const auto* typedInput = dynamic_cast<const ColumnVector<Arg>*>(input);
    bioassert(typedInput, "Function operand has an unexpected column type.");
    auto* output = static_cast<ColumnOptVector<JustRes>*>(result);

    const std::vector<Arg>& inputRaw = typedInput->getRaw();
    const size_t size = inputRaw.size();

    output->resize(size);
    auto& outputRaw = output->getRaw();

    Functor functor = makeFunctor<Functor>(context, memory);
    for (size_t row = 0; row < size; row++) {
        if (inputRaw[row].isValid()) {
            outputRaw[row] = functor(inputRaw[row]);
        } else {
            outputRaw[row] = std::nullopt;
        }
    }
}

// The sibling of functionEntityKernel for a function whose answer is an entity too: a node
// column spells its null as an invalid ID, so an unmatched edge writes one straight out
// rather than needing a nullable column to hold it.
template <typename Functor>
void functionEntityToEntityKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    const auto* typedInput = dynamic_cast<const ColumnVector<Arg>*>(input);
    bioassert(typedInput, "Function operand has an unexpected column type.");
    auto* output = static_cast<ColumnVector<Res>*>(result);

    const std::vector<Arg>& inputRaw = typedInput->getRaw();
    const size_t size = inputRaw.size();

    output->resize(size);
    auto& outputRaw = output->getRaw();

    Functor functor = makeFunctor<Functor>(context, memory);
    for (size_t row = 0; row < size; row++) {
        if (inputRaw[row].isValid()) {
            outputRaw[row] = functor(inputRaw[row]);
        } else {
            outputRaw[row] = Res {};
        }
    }
}

template <typename Functor, typename Element>
void applyFunctionOverOptVector(Functor& functor,
                                const ColumnOptVector<Element>* input,
                                ColumnOptVector<TypeUtils::unwrap_optional_t<typename Functor::ResultType>>* output) {
    const auto& inputRaw = input->getRaw();
    const size_t size = inputRaw.size();

    output->resize(size);
    auto& outputRaw = output->getRaw();

    for (size_t row = 0; row < size; row++) {
        if (inputRaw[row].has_value()) {
            outputRaw[row] = functor(inputRaw[row].value());
        } else {
            outputRaw[row] = std::nullopt;
        }
    }
}

template <typename Functor>
void functionOptKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;
    using JustRes = TypeUtils::unwrap_optional_t<Res>;

    auto* output = static_cast<ColumnOptVector<JustRes>*>(result);
    Functor functor = makeFunctor<Functor>(context, memory);

    if (const auto* typedInput = dynamic_cast<const ColumnOptVector<Arg>*>(input)) {
        applyFunctionOverOptVector(functor, typedInput, output);
        return;
    }

    bioassert(false, "Function operand has an unexpected column type.");
}

// Whether a column holds this element, in any of the shapes a chunk column takes: a plain
// vector, a nullable one, or a constant of either.
template <typename Element>
bool columnHoldsElement(const Column* column) {
    return dynamic_cast<const ColumnVector<Element>*>(column)
           || dynamic_cast<const ColumnVector<std::optional<Element>>*>(column)
           || dynamic_cast<const ColumnConst<Element>*>(column)
           || dynamic_cast<const ColumnConst<std::optional<Element>>*>(column);
}

template <ColumnOperator Op>
struct BinaryOpTraits;

template <>
struct BinaryOpTraits<OP_ADD> {
    using Functor = Add;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_CONCAT> {
    using Functor = Concat;
};

template <>
struct BinaryOpTraits<OP_INDEX> {
    using Functor = ListIndex;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_SUB> {
    using Functor = Sub;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_MUL> {
    using Functor = Mul;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_EQUAL> {
    using Functor = Eq;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_NOT_EQUAL> {
    using Functor = Ne;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_DIV> {
    using Functor = Div;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_MOD> {
    using Functor = Mod;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_POW> {
    using Functor = Pow;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_GREATER_THAN> {
    using Functor = Gt;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_LESS_THAN> {
    using Functor = Lt;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_GREATER_THAN_OR_EQUAL> {
    using Functor = Gte;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_LESS_THAN_OR_EQUAL> {
    using Functor = Lte;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_AND> {
    using Functor = And;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_OR> {
    using Functor = Or;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_XOR> {
    using Functor = Xor;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_STARTS_WITH> {
    using Functor = StartsWith;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_ENDS_WITH> {
    using Functor = EndsWith;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_CONTAINS> {
    using Functor = Contains;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_IN> {
    using Functor = In;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_INDEX_OF> {
    using Functor = IndexOf;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_FUNC_COSINE_SIMILARITY> {
    using Functor = CosineSimilarityFunction;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        ColumnFunctions::exec<CosineSimilarity>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_FUNC_EUCLIDEAN_DISTANCE> {
    using Functor = EuclideanDistanceFunction;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        ColumnFunctions::exec<EuclideanDistance>(result, lhs, rhs);
    }
};

template <ColumnOperator Op, typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers*) {
        BinaryOpTraits<Op>::exec(static_cast<ResCol*>(result),
                                 static_cast<const LhsCol*>(lhs),
                                 static_cast<const RhsCol*>(rhs));
    }
};

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_CONCAT, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
        BinaryOperators::exec<Concat>(static_cast<ResCol*>(result),
                                      static_cast<const LhsCol*>(lhs),
                                      static_cast<const RhsCol*>(rhs),
                                      Concat {&mem->stringBuffer(), &mem->listBuffer()});
    }
};

template <typename Op, typename ResCol, typename LhsCol, typename RhsCol>
void runArithmeticOp(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
    BinaryOperators::exec<Op>(static_cast<ResCol*>(result),
                              static_cast<const LhsCol*>(lhs),
                              static_cast<const RhsCol*>(rhs),
                              Op {&mem->listBuffer(), &mem->stringBuffer()});
}

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_ADD, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
        runArithmeticOp<Add, ResCol, LhsCol, RhsCol>(result, lhs, rhs, mem);
    }
};

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_SUB, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
        runArithmeticOp<Sub, ResCol, LhsCol, RhsCol>(result, lhs, rhs, mem);
    }
};

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_MUL, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
        runArithmeticOp<Mul, ResCol, LhsCol, RhsCol>(result, lhs, rhs, mem);
    }
};

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_DIV, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
        runArithmeticOp<Div, ResCol, LhsCol, RhsCol>(result, lhs, rhs, mem);
    }
};

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_MOD, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
        runArithmeticOp<Mod, ResCol, LhsCol, RhsCol>(result, lhs, rhs, mem);
    }
};

template <ColumnOperator Op, typename ResCol, typename LhsCol, typename RhsCol>
void applyBinaryOp(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
    BinaryOpKernel<Op, ResCol, LhsCol, RhsCol>::run(result, lhs, rhs, mem);
}

template <ColumnOperator Op>
struct BinaryOpSelector {
    LocalMemory* _memory {nullptr};
    Column* _result {nullptr};
    NLBinaryFn _fn {nullptr};

    template <typename LhsCol, typename RhsCol>
    void operator()(const LhsCol*, const RhsCol*) {
        using OpType = typename BinaryOpTraits<Op>::Functor;
        using ResCol = ColumnCombination<OpType, LhsCol, RhsCol>;
        using ResColType = ResCol::ResultColumnType;

        _result = _memory->alloc<ResColType>();
        _fn = &applyBinaryOp<Op, ResColType, LhsCol, RhsCol>;
    }
};

template <typename Primitive, typename ResCol, typename LhsCol, typename RhsCol>
void applyValueListIndex(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* memory) {
    BinaryOperators::exec<ValueListIndex<Primitive>>(static_cast<ResCol*>(result),
                                                     static_cast<const LhsCol*>(lhs),
                                                     static_cast<const RhsCol*>(rhs));
}

template <typename Primitive>
struct ValueListIndexSelector {
    LocalMemory* _memory {nullptr};
    Column* _result {nullptr};
    NLBinaryFn _fn {nullptr};

    template <typename LhsCol, typename RhsCol>
    void operator()(const LhsCol*, const RhsCol*) {
        using ResCol = ColumnCombination<ValueListIndex<Primitive>, LhsCol, RhsCol>;
        using ResColType = ResCol::ResultColumnType;

        _result = _memory->alloc<ResColType>();
        _fn = &applyValueListIndex<Primitive, ResColType, LhsCol, RhsCol>;
    }
};

// The entity sibling of ValueListIndex: an ID column spells a null entity as an invalid
// ID, so a position past the end - or the tagged null a list holds for an entity an
// OPTIONAL MATCH did not match - reads back as one rather than as an absent optional.
template <typename IDType>
struct EntityListIndex {
    template <typename List, typename Index>
    IDType operator()(const List& list, const Index& index) const {
        return ValueListIndex<IDType> {}(list, index).value_or(IDType {});
    }
};

template <typename IDType, typename ResCol, typename LhsCol, typename RhsCol>
void applyEntityListIndex(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers*) {
    BinaryOpExecutor<EntityListIndex<IDType>>::apply(static_cast<ResCol*>(result),
                                                     static_cast<const LhsCol*>(lhs),
                                                     static_cast<const RhsCol*>(rhs));
}

template <typename IDType>
struct EntityListIndexSelector {
    LocalMemory* _memory {nullptr};
    Column* _result {nullptr};
    NLBinaryFn _fn {nullptr};

    template <typename LhsCol, typename RhsCol>
    void operator()(const LhsCol*, const RhsCol*) {
        using Lists = typename InnerTypeHelper<LhsCol>::type;
        using Indices = typename InnerTypeHelper<RhsCol>::type;

        constexpr bool readsAList = std::is_same_v<TypeUtils::unwrap_optional_t<Lists>, ListView>;
        constexpr bool readsConstants = std::is_same_v<LhsCol, ColumnConst<Lists>>
                                     && std::is_same_v<RhsCol, ColumnConst<Indices>>;
        using ResCol = std::conditional_t<readsConstants, ColumnConst<IDType>, ColumnVector<IDType>>;

        if constexpr (readsAList) {
            _result = _memory->alloc<ResCol>();
            _fn = &applyEntityListIndex<IDType, ResCol, LhsCol, RhsCol>;
        }
    }
};

template <typename IDType>
NLBinaryFn selectEntityListIndexOf(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result) {
    using Pairs = PairRestrictions<OP_INDEX>;

    EntityListIndexSelector<IDType> selector {._memory = memory};
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           EntityListIndexSelector<IDType>,
                           typename Pairs::Excluded>::dispatch(lhs, rhs, selector);

    result = selector._result;
    return selector._fn;
}

// Execute a body of statements
void runBody(NLExecutionContext* context, const NLStmtContainer* body) {
    for (const NLFunctionDescriptor& descriptor : body->stmts()) {
        const auto func = descriptor.getFunction();
        NLFunctionData* funcData = descriptor.getData();
        func(context, funcData);
    }
}

template <SupportedType T>
typename T::Primitive propertyScanLiteralAs(const NLScanByPropertyValueLoopData::Literal& literal) {
    if constexpr (std::same_as<T, types::String>) {
        return std::get<std::string>(literal);
    } else {
        return std::get<typename T::Primitive>(literal);
    }
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

// Gather rows of a carried column by applying indices
template <typename ElementType>
void gatherColumn(const Column* input,
                  const ColumnVector<size_t>* indices,
                  Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    typedOutput->resize(indices->size());
    const auto& indicesRaw = indices->getRaw();
    auto& typedInputRaw = typedInput->getRaw();
    auto& typedOutputRaw = typedOutput->getRaw();

    for (size_t row = 0; row < indicesRaw.size(); row++) {
        typedOutputRaw[row] = typedInputRaw[indicesRaw[row]];
    }
}

template <typename Element>
void gatherOptColumn(const Column* input,
                     const ColumnVector<size_t>* indices,
                     Column* output) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    const std::vector<size_t>& indicesRaw = indices->getRaw();
    typedOutput->resize(indicesRaw.size());

    for (size_t row = 0; row < indicesRaw.size(); row++) {
        typedOutput->setOptional(row, typedInput->get(indicesRaw[row]));
    }
}

// Fill a chunk with a run of null rows. Every element type reads its own
// default-constructed value as null: an ID defaults to the invalid ID that ID::isValid
// rejects, which is how an entity an OPTIONAL MATCH did not match is spelled.
template <typename ElementType>
void fillNullColumn(Column* output, size_t rowCount) {
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    typedOutput->getRaw().assign(rowCount, ElementType {});
}

template <typename Element>
void fillNullOptColumn(Column* output, size_t rowCount) {
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    typedOutput->clear();
    typedOutput->resize(rowCount);
}

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

std::optional<NodeID> namedNode(NodeID node) {
    return node;
}

std::optional<NodeID> namedNode(types::Int64::Primitive id) {
    if (id < 0) {
        return std::nullopt;
    }

    return NodeID(static_cast<uint64_t>(id));
}

std::optional<NodeID> namedNode(types::UInt64::Primitive id) {
    return NodeID(id);
}

std::optional<NodeID> namedNode(CellView cell) {
    switch (cell.getTag()) {
        case CellTag::Int:
            return namedNode(cell.getAs<types::Int64::Primitive>());
        break;

        case CellTag::UInt:
            return namedNode(cell.getAs<types::UInt64::Primitive>());
        break;

        case CellTag::NodeID:
            return cell.getAs<NodeID>();
        break;

        default:
            return std::nullopt;
        break;
    }
}

template <typename Cell>
std::optional<NodeID> namedNode(const std::optional<Cell>& cell) {
    if (!cell.has_value()) {
        return std::nullopt;
    }

    return namedNode(*cell);
}

template <typename Cell>
void fetchNamedNodes(const Column* ids, const NLExecutionContext* context, ColumnVector<size_t>* indices, ColumnNodeIDs* nodes) {
    const std::vector<Cell>& cells = static_cast<const ColumnVector<Cell>*>(ids)->getRaw();
    std::vector<size_t>& rows = indices->getRaw();
    std::vector<NodeID>& fetched = nodes->getRaw();

    const ScannedNodes scanned(*context->getView(), context->getWriteBuffer(), context->getFirstQueryNode());

    for (size_t row = 0; row < cells.size(); row++) {
        const std::optional<NodeID> node = namedNode(cells[row]);

        if (node.has_value() && scanned.contains(*node)) {
            rows.push_back(row);
            fetched.push_back(*node);
        }
    }
}

bool isMaskConstant(const Column* mask) {
    return mask->getKind() == ColumnConst<CustomBool>::staticKind();
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

    typedResult->resize(operandRaw.size());
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

// Block-repeat: the outer column of a cross product, where each outer row pairs
// with the whole inner chunk, so pair p takes input row p/factor. Fills the
// `rowCount` pairs from `position` on, which may start partway into one input
// row's block and end partway into another's.
template <typename ElementType>
void blockRepeatColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    auto outputIt = outputRaw.begin();
    size_t rowsLeft = rowCount;
    size_t inputIndex = position / factor;
    size_t doneInBlock = position % factor;

    while (rowsLeft > 0 && inputIndex < inputRaw.size()) {
        const size_t count = std::min(factor - doneInBlock, rowsLeft);
        std::fill_n(outputIt, count, inputRaw[inputIndex]);

        outputIt += count;
        rowsLeft -= count;
        inputIndex++;
        doneInBlock = 0;
    }
}

template <typename Element>
void blockRepeatOptColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    const size_t inputSize = typedInput->size();
    typedOutput->resize(rowCount);

    size_t outputRow = 0;
    size_t inputIndex = position / factor;
    size_t doneInBlock = position % factor;

    while (outputRow < rowCount && inputIndex < inputSize) {
        const size_t blockEnd = outputRow + std::min(factor - doneInBlock, rowCount - outputRow);
        const auto value = typedInput->get(inputIndex);

        for (; outputRow < blockEnd; outputRow++) {
            typedOutput->setOptional(outputRow, value);
        }

        inputIndex++;
        doneInBlock = 0;
    }
}

void blockRepeatConstColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    output->assignFromLine(input, 0, rowCount);
}

// Tile: the inner column of a cross product, where the inner chunk repeats once
// per outer row, so pair p takes input row p % M. Fills the `rowCount` pairs from
// `position` on, which may start partway into one tile and end partway into
// another. M is the tile's own length, which is what the enclosing product passes
// as the factor.
template <typename ElementType>
void tileColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    const size_t tileLength = inputRaw.size();
    if (tileLength == 0) {
        return;
    }

    auto outputIt = outputRaw.begin();
    size_t rowsLeft = rowCount;
    size_t inputIndex = position % tileLength;

    while (rowsLeft > 0) {
        const size_t count = std::min(tileLength - inputIndex, rowsLeft);
        outputIt = std::copy(inputRaw.begin() + inputIndex, inputRaw.begin() + inputIndex + count, outputIt);

        rowsLeft -= count;
        inputIndex = 0;
    }
}

template <typename Element>
void tileOptColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    typedOutput->resize(rowCount);

    const size_t tileLength = typedInput->size();
    if (tileLength == 0) {
        return;
    }

    size_t outputRow = 0;
    size_t inputIndex = position % tileLength;

    while (outputRow < rowCount) {
        const size_t count = std::min(tileLength - inputIndex, rowCount - outputRow);

        for (size_t offset = 0; offset < count; offset++) {
            typedOutput->setOptional(outputRow + offset, typedInput->get(inputIndex + offset));
        }

        outputRow += count;
        inputIndex = 0;
    }
}

// Constant broadcast: the one value a ColumnConst holds is laid out over every row
// of the step, as a present value. The two broadcasts above repeat the several
// values of a chunk; a constant column has no rows of its own to repeat, so its row
// count comes from the driving relation and its value from the column itself.
template <typename Primitive>
void broadcastConstantColumn(const Column* value, size_t rowCount, Column* output) {
    const ColumnConst<Primitive>* typedValue = static_cast<const ColumnConst<Primitive>*>(value);
    ColumnOptVector<Primitive>* typedOutput = static_cast<ColumnOptVector<Primitive>*>(output);

    const Primitive& constant = typedValue->getRaw();
    typedOutput->resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        typedOutput->set(row, constant);
    }
}

template <typename Primitive>
void broadcastNullableConstantColumn(const Column* value, size_t rowCount, Column* output) {
    const auto* typedValue = static_cast<const ColumnConst<std::optional<Primitive>>*>(value);
    ColumnOptVector<Primitive>* typedOutput = static_cast<ColumnOptVector<Primitive>*>(output);

    const auto constant = typedValue->get(0);
    typedOutput->resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        typedOutput->setOptional(row, constant);
    }
}

// A constant chunk is not always a ColumnConst: a kernel computing over literals writes
// its one row into an ordinary column. That row is the one every row of the step reads.
template <typename Primitive>
void broadcastSingleRowColumn(const Column* value, size_t rowCount, Column* output) {
    const ColumnOptVector<Primitive>* typedValue = static_cast<const ColumnOptVector<Primitive>*>(value);
    ColumnOptVector<Primitive>* typedOutput = static_cast<ColumnOptVector<Primitive>*>(output);

    if (typedValue->empty()) {
        typedOutput->clear();
        typedOutput->resize(rowCount);
        return;
    }

    const auto constant = typedValue->get(0);
    typedOutput->resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        typedOutput->setOptional(row, constant);
    }
}

// The null literal laid out over every row of the step: it holds no value to repeat, so
// each row is the absent value. An untyped null is carried as a null integer, which is
// the column the layout fills.
void broadcastNullColumn(const Column* value, size_t rowCount, Column* output) {
    ColumnOptVector<int64_t>* typedOutput = static_cast<ColumnOptVector<int64_t>*>(output);

    typedOutput->clear();
    typedOutput->resize(rowCount);
}

// The plain sibling of broadcastConstantColumn: a list, a map, a map entry, a node, an edge
// or a path carries no absence of its own as a value does, so the rows are a plain column
// of the one element the constant holds.
template <typename Element>
void broadcastConstantPlainColumn(const Column* value, size_t rowCount, Column* output) {
    const ColumnConst<Element>* typedValue = static_cast<const ColumnConst<Element>*>(value);
    ColumnVector<Element>* typedOutput = static_cast<ColumnVector<Element>*>(output);

    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    std::fill_n(outputRaw.begin(), rowCount, typedValue->getRaw());
}

// The one row a computation over constants wrote, held as the constant it stands for
template <typename Element>
void readSingleRowAsConstant(Column* result, const Column* row) {
    const std::vector<Element>& rowRaw = static_cast<const ColumnVector<Element>*>(row)->getRaw();
    bioassert(rowRaw.size() == 1, "A computation over constants did not write a single row.");

    static_cast<ColumnConst<Element>*>(result)->set(rowRaw.front());
}

template <typename Element>
NLUnaryFn selectRowConstantOf(const Column* row, LocalMemory* memory, Column*& result) {
    bioassert(row->getKind() == ColumnVector<Element>::staticKind(),
              "nl.row_constant reads a {} under a chunk of another element type",
              row->getTypeName());

    result = memory->alloc<ColumnConst<Element>>();
    return &readSingleRowAsConstant<Element>;
}

template <typename Element>
void readOptSingleRowAsConstant(Column* result, const Column* row) {
    const ColumnOptVector<Element>* typedRow = static_cast<const ColumnOptVector<Element>*>(row);
    bioassert(typedRow->size() == 1, "A computation over constants did not write a single row.");

    static_cast<ColumnConst<std::optional<Element>>*>(result)->setOptional(typedRow->get(0));
}

template <typename Element>
NLUnaryFn selectOptRowConstantOf(const Column* row, LocalMemory* memory, Column*& result) {
    bioassert(row->getKind() == ColumnOptVector<Element>::staticKind(),
              "nl.row_constant reads a {} under a chunk of another element type",
              row->getTypeName());

    result = memory->alloc<ColumnConst<std::optional<Element>>>();
    return &readOptSingleRowAsConstant<Element>;
}

// Range copy: rows [inputOffset, inputOffset + rowCount) of the input land at
// output indices [0, rowCount). This lifts a skip's surviving suffix to the front
// of a fresh chunk - nl.skip_truncate passes inputOffset = skipThisStep and
// rowCount = emitThisStep. std::copy of a contiguous range is lowered to memcpy.
template <typename ElementType>
void copyRangeColumn(const Column* input, size_t inputOffset, size_t rowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    const auto first = inputRaw.begin() + inputOffset;
    std::copy(first, first + rowCount, outputRaw.begin());
}

template <typename Element>
void copyRangeOptColumn(const Column* input, size_t inputOffset, size_t rowCount, Column* output) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    typedOutput->resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        typedOutput->setOptional(row, typedInput->get(inputOffset + row));
    }
}

void copyRangeConstColumn(const Column* input, size_t inputOffset, size_t rowCount, Column* output) {
    output->assignFromLine(input, inputOffset, rowCount);
}

void gatherConstColumn(const Column* input, const ColumnVector<size_t>* indices, Column* output) {
    output->assignFromLine(input, 0, indices->size());
}

// Append every row of an input chunk onto the tail of a growing buffer of the
// same element type. nl.sort_collect calls this once per producing-loop step, so
// the buffer accumulates every row across all chunks, row-aligned with the other
// buffers of the same accumulator.
template <typename ElementType>
void appendColumn(const Column* input, Column* buffer) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedBuffer = static_cast<ColumnVector<ElementType>*>(buffer);

    const auto& inputRaw = typedInput->getRaw();
    auto& bufferRaw = typedBuffer->getRaw();

    bufferRaw.insert(bufferRaw.end(), inputRaw.begin(), inputRaw.end());
}

template <typename Element>
void appendOptColumn(const Column* input, Column* buffer) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedBuffer = static_cast<ColumnOptVector<Element>*>(buffer);

    const size_t inputSize = typedInput->size();
    const size_t firstRow = typedBuffer->size();
    typedBuffer->resize(firstRow + inputSize);

    for (size_t row = 0; row < inputSize; row++) {
        typedBuffer->setOptional(firstRow + row, typedInput->get(row));
    }
}

// The append of a column of views, for a buffer that outlives the loop filling it: the
// views a chunk carries point into the buffers that built them, which the producing step
// empties when it comes round again, so the values themselves go into the accumulator's own.
template <typename ElementType>
void appendOwnedColumn(const Column* input, Column* buffer, NLValueStore& values) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedBuffer = static_cast<ColumnVector<ElementType>*>(buffer);

    const auto& inputRaw = typedInput->getRaw();
    auto& bufferRaw = typedBuffer->getRaw();

    const size_t firstRow = bufferRaw.size();
    bufferRaw.resize(firstRow + inputRaw.size());

    for (size_t row = 0; row < inputRaw.size(); row++) {
        bufferRaw[firstRow + row] = values.ownElement(inputRaw[row]);
    }
}

template <typename Element>
void appendOwnedOptColumn(const Column* input, Column* buffer, NLValueStore& values) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedBuffer = static_cast<ColumnOptVector<Element>*>(buffer);

    const size_t inputSize = typedInput->size();
    const size_t firstRow = typedBuffer->size();
    typedBuffer->resize(firstRow + inputSize);

    for (size_t row = 0; row < inputSize; row++) {
        if (typedInput->isValid(row)) {
            typedBuffer->set(firstRow + row, values.own(typedInput->getValue(row)));
        }
    }
}

void appendCollectedChunk(const NLSortCollectData::Append& append, NLValueStore& values) {
    if (append._appendOwned) {
        append._appendOwned(append._input, append._buffer, values);
    } else {
        append._append(append._input, append._buffer);
    }
}

template <typename Primitive>
bool sortsBefore(const Primitive& lhs, const Primitive& rhs) {
    return lhs < rhs;
}

// Cypher orders a NaN after every number, where < answers false against it
bool sortsBefore(double lhs, double rhs) {
    if (std::isnan(lhs)) {
        return false;
    }

    return std::isnan(rhs) || lhs < rhs;
}

// 3-way compare two rows of a non-null orderable column (an ID column, or a plain scalar
// a procedure yielded): negative if row a sorts before row b, positive if after, zero if
// they are equal.
template <typename ElementType>
int compareColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    const ElementType& valueA = raw[a];
    const ElementType& valueB = raw[b];

    if (sortsBefore(valueA, valueB)) {
        return -1;
    } else if (sortsBefore(valueB, valueA)) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a nullable value column. A null sorts after every
// value, so an ascending order places nulls last (matching Cypher's ORDER BY),
// and two nulls tie; non-null values compare by their natural order.
template <typename Primitive>
int compareOptColumn(const Column* column, size_t a, size_t b) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);

    const bool aNull = !typedColumn->isValid(a);
    const bool bNull = !typedColumn->isValid(b);
    if (aNull || bNull) {
        if (aNull && bNull) {
            return 0;
        }

        return aNull ? 1 : -1;
    }

    const Primitive& valueA = typedColumn->getValue(a);
    const Primitive& valueB = typedColumn->getValue(b);

    if (sortsBefore(valueA, valueB)) {
        return -1;
    } else if (sortsBefore(valueB, valueA)) {
        return 1;
    }

    return 0;
}

// Copy a plain value column into a nullable one with every row present (nl.to_nullable),
// so a kernel reading a nullable value column takes a column a procedure yielded.
template <typename Primitive>
void toNullableColumn(Column* result, const Column* operand) {
    const std::vector<Primitive>& values = static_cast<const ColumnVector<Primitive>*>(operand)->getRaw();
    ColumnOptVector<Primitive>* nullables = static_cast<ColumnOptVector<Primitive>*>(result);

    nullables->resize(values.size());

    for (size_t row = 0; row < values.size(); row++) {
        nullables->set(row, values[row]);
    }
}

// Read an entity column as a nullable column of its IDs' integers: a node or an edge an
// OPTIONAL MATCH did not match carries an invalid ID, which is the null. The entity
// sibling of toNullableColumn, for the column family whose null is not an absent optional.
template <typename ID>
void entityToNullableColumn(Column* result, const Column* operand) {
    const std::vector<ID>& ids = static_cast<const ColumnVector<ID>*>(operand)->getRaw();
    ColumnOptVector<types::UInt64::Primitive>* nullables = static_cast<ColumnOptVector<types::UInt64::Primitive>*>(result);

    nullables->resize(ids.size());
    for (size_t row = 0; row < ids.size(); row++) {
        if (ids[row].isValid()) {
            nullables->set(row, ids[row].getValue());
        } else {
            nullables->setNull(row);
        }
    }
}

template <typename ID>
ID checkedEntity(const CellView cell) {
    const CellTag tag = cell.getTag();

    if (tag == TypeToCellTag<ID>::Tag) {
        return cell.getAs<ID>();
    } else if (tag == CellTag::Null) {
        return ID {};
    }

    constexpr std::string_view ENTITY = std::is_same_v<ID, NodeID> ? "a node" : "a relationship";
    throw IRException(fmt::format("A pattern reads {}, not '{}'", ENTITY, queryTypeName(tag)));
}

template <typename ID>
void checkedCastColumn(Column* result, const Column* operand) {
    const std::vector<CellView>& cells = static_cast<const ColumnVector<CellView>*>(operand)->getRaw();
    std::vector<ID>& ids = static_cast<ColumnVector<ID>*>(result)->getRaw();

    ids.resize(cells.size());
    std::transform(cells.begin(), cells.end(), ids.begin(), [](const CellView cell) {
        return checkedEntity<ID>(cell);
    });
}

template <typename ID>
NLUnaryFn selectCheckedCastOf(const Column* operand, LocalMemory* memory, Column*& result) {
    const ColumnKind::Code kind = operand->getKind();

    if (kind == ColumnVector<CellView>::staticKind()) {
        result = memory->alloc<ColumnVector<ID>>();
        return &checkedCastColumn<ID>;
    }

    throw IRException("A pattern reads its node or relationship out of a column of tagged cells laid out over its rows");
}

template <typename Primitive>
void toNullableConst(Column* result, const Column* operand) {
    const auto* constant = static_cast<const ColumnConst<Primitive>*>(operand);
    auto* nullable = static_cast<ColumnConst<std::optional<Primitive>>*>(result);

    if (constant->empty()) {
        nullable->clear();
        return;
    }

    nullable->setOptional((*constant)[0]);
}

template <typename Primitive>
NLUnaryFn selectToNullableOf(const Column* operand, LocalMemory* memory, Column*& result) {
    if (operand->getContainerKind() == ContainerKind::code<ColumnConst>()) {
        result = memory->alloc<ColumnConst<std::optional<Primitive>>>();
        return &toNullableConst<Primitive>;
    }

    result = memory->alloc<ColumnOptVector<Primitive>>();
    return &toNullableColumn<Primitive>;
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
    const ColumnOptVector<CustomBool>* typedOperand = static_cast<const ColumnOptVector<CustomBool>*>(operand);
    std::vector<CustomBool>& resultRaw = static_cast<ColumnVector<CustomBool>*>(result)->getRaw();

    resultRaw.resize(typedOperand->size());
    for (size_t row = 0; row < resultRaw.size(); row++) {
        resultRaw[row] = CustomBool {!(typedOperand->isValid(row) && typedOperand->getValue(row))};
    }
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

template <typename ElementType, bool TESTS_NULL>
void applyNullTestOnVector(Column* result, const Column* operand) {
    const std::vector<ElementType>& operandRaw = static_cast<const ColumnVector<ElementType>*>(operand)->getRaw();
    std::vector<CustomBool>& resultRaw = static_cast<ColumnVector<CustomBool>*>(result)->getRaw();

    resultRaw.resize(operandRaw.size());
    std::transform(operandRaw.begin(), operandRaw.end(), resultRaw.begin(), [](const ElementType& value) {
        return CustomBool {holdsAValue(value) != TESTS_NULL};
    });
}

template <typename ElementType, bool TESTS_NULL>
void applyNullTestOnConst(Column* result, const Column* operand) {
    const ElementType& value = static_cast<const ColumnConst<ElementType>*>(operand)->getRaw();
    static_cast<ColumnConst<CustomBool>*>(result)->set(CustomBool {holdsAValue(value) != TESTS_NULL});
}

template <bool TESTS_NULL>
struct NullTestSelector {
    LocalMemory* _memory {nullptr};
    NLUnaryFn _fn {nullptr};
    Column* _result {nullptr};

    template <typename ElementType>
    void operator()(const ColumnVector<ElementType>* operand) {
        _result = _memory->alloc<ColumnVector<CustomBool>>();
        _fn = &applyNullTestOnVector<ElementType, TESTS_NULL>;
    }

    template <typename ElementType>
    void operator()(const ColumnConst<ElementType>* operand) {
        _result = _memory->alloc<ColumnConst<CustomBool>>();
        _fn = &applyNullTestOnConst<ElementType, TESTS_NULL>;
    }

    template <typename ElementType>
    void operator()(const ColumnSet<ElementType>* operand) {
        throw IRException("A set column holds no rows to test for null");
    }
};

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

template <typename IDType>
ListView listPathEntities(const PathView path,
                          size_t count,
                          IDType (PathView::*readEntity)(size_t) const,
                          CellTag tag,
                          QueryListBuffer& buffer) {
    ListWriteCursor cursor = buffer.reserveList(count, count * sizeof(IDType));

    for (size_t index = 0; index < count; index++) {
        cursor.writeValueAt(index, tag, (path.*readEntity)(index));
    }

    return cursor.getView();
}

std::optional<PathView> columnPath(const Column* paths, size_t row) {
    const GraphPath& path = (*static_cast<const ColumnVector<GraphPath>*>(paths))[row];
    if (path.isNull()) {
        return std::nullopt;
    }

    return PathView {path.getIDs()};
}

// The path a tagged cell holds, or nothing where it holds a null. Anything else is a path
// function applied to something that is no path, which only the cell's tag tells per row.
std::optional<PathView> taggedPath(const CellView cell) {
    const CellTag tag = cell.getTag();

    if (tag == CellTag::Path) {
        return cell.getAs<PathView>();
    } else if (tag == CellTag::Null) {
        return std::nullopt;
    }

    throw IRException(fmt::format("nodes(), relationships() and length() read a path, not '{}'", queryTypeName(tag)));
}

std::optional<PathView> taggedColumnPath(const Column* paths, size_t row) {
    return taggedPath((*static_cast<const ColumnVector<CellView>*>(paths))[row]);
}

void readPathElements(const Column* paths,
                      NLPathReadFunction read,
                      PathElementsKind kind,
                      Column* output,
                      QueryListBuffer& listBuffer) {
    const size_t rowCount = paths->size();

    if (kind == PathElementsKind::Length) {
        std::vector<std::optional<uint64_t>>& lengths = static_cast<ColumnOptVector<uint64_t>*>(output)->getRaw();
        lengths.resize(rowCount);

        for (size_t row = 0; row < rowCount; row++) {
            const std::optional<PathView> path = read(paths, row);
            if (!path) {
                lengths[row] = std::nullopt;
            } else {
                lengths[row] = path->getLength();
            }
        }

        return;
    }

    std::vector<std::optional<ListView>>& lists = static_cast<ColumnOptVector<ListView>*>(output)->getRaw();
    lists.resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        const std::optional<PathView> path = read(paths, row);
        if (!path) {
            lists[row] = std::nullopt;
        } else if (kind == PathElementsKind::Nodes) {
            lists[row] = listPathEntities(*path, path->getNodeCount(), &PathView::getNode, CellTag::NodeID, listBuffer);
        } else {
            lists[row] = listPathEntities(*path, path->getLength(), &PathView::getEdge, CellTag::EdgeID, listBuffer);
        }
    }
}

// The truth value a list predicate holds for a row, from how many of its elements held,
// failed or answered null: an unknown element leaves the answer unknown unless the known
// ones already decide it
std::optional<bool> decideListPredicate(ListPredicateKind kind, size_t trues, size_t falses, size_t nulls) {
    switch (kind) {
        case ListPredicateKind::All:
            if (falses > 0) {
                return false;
            } else if (nulls > 0) {
                return std::nullopt;
            }

            return true;
        break;

        case ListPredicateKind::Any:
            if (trues > 0) {
                return true;
            } else if (nulls > 0) {
                return std::nullopt;
            }

            return false;
        break;

        case ListPredicateKind::None:
            if (trues > 0) {
                return false;
            } else if (nulls > 0) {
                return std::nullopt;
            }

            return true;
        break;

        case ListPredicateKind::Single:
            if (trues > 1) {
                return false;
            } else if (nulls > 0) {
                return std::nullopt;
            }

            return trues == 1;
        break;
    }

    return std::nullopt;
}

bool isListPredicateDecided(ListPredicateKind kind, size_t trues, size_t falses) {
    switch (kind) {
        case ListPredicateKind::All:
            return falses > 0;
        break;

        case ListPredicateKind::Any:
            return trues > 0;
        break;

        case ListPredicateKind::None:
            return trues > 0;
        break;

        case ListPredicateKind::Single:
            return trues > 1;
        break;
    }

    return false;
}

// Runs the body of an op over the elements of one row's list a chunk of (row, element)
// pairs at a time, and hands @param takeYield the tags of the elements each chunk yielded
template <typename TakeYield>
void runElementBody(NLExecutionContext* context, NLElementBodyData* data, NLElementCursor& cursor, TakeYield takeYield) {
    const Column* source = data->getSource();
    const NLStmtContainer* body = data->getStmts();
    const NLUnwindElementEmitFunction elementEmit = data->getElementEmitFunc();
    const size_t chunkSize = context->getChunkSize();

    ColumnVector<size_t>* rows = data->getRows();
    ColumnVector<size_t>* positions = data->getPositions();
    ColumnVector<uint64_t>* rowTags = data->getRowTags();

    while (!cursor.exhausted()) {
        cursor.fillChunk(chunkSize, rows->getRaw(), elementEmit ? &positions->getRaw() : nullptr);

        // The tag of an element is the row the pair came from: the body hands back the
        // tags of the elements its WHERE kept
        const std::vector<size_t>& rowsRaw = rows->getRaw();
        std::vector<uint64_t>& rowTagsRaw = rowTags->getRaw();
        rowTagsRaw.assign(rowsRaw.begin(), rowsRaw.end());

        if (elementEmit) {
            elementEmit(source, rows, positions, data->getElementOutput());
        }

        for (const NLCarriedColumn& carriedColumn : data->carriedColumns()) {
            const auto gatherFunc = carriedColumn.getGatherFunc();
            gatherFunc(carriedColumn.getInput(), rows, carriedColumn.getOutput());
        }

        runBody(context, body);

        const std::vector<uint64_t>& yieldedRaw =
            static_cast<const ColumnVector<uint64_t>*>(data->getYieldedRowTags())->getRaw();

        bioassert(data->getValue()->size() == yieldedRaw.size(),
                  "Yielded value of an op over list elements is not row-aligned with its row tags.");

        takeYield(yieldedRaw);
    }
}

template <typename Primitive>
void caseWriteOptCell(Column* result, size_t resultRow, const Column* value, size_t valueRow) {
    std::vector<std::optional<Primitive>>& results = static_cast<ColumnOptVector<Primitive>*>(result)->getRaw();
    results[resultRow] = static_cast<const ColumnOptVector<Primitive>*>(value)->getRaw()[valueRow];
}

template <typename Primitive>
void caseWritePlainCell(Column* result, size_t resultRow, const Column* value, size_t valueRow) {
    std::vector<std::optional<Primitive>>& results = static_cast<ColumnOptVector<Primitive>*>(result)->getRaw();
    results[resultRow] = static_cast<const ColumnVector<Primitive>*>(value)->getRaw()[valueRow];
}

template <typename Primitive>
void caseWriteNull(Column* result, size_t resultRow, const Column* value, size_t valueRow) {
    static_cast<ColumnOptVector<Primitive>*>(result)->getRaw()[resultRow] = std::nullopt;
}

template <typename Element>
void caseWriteOwnNull(Column* result, size_t resultRow, const Column* value, size_t valueRow) {
    static_cast<ColumnVector<Element>*>(result)->getRaw()[resultRow] = Element {};
}

void caseWriteNullTaggedCell(Column* result, size_t resultRow, const Column* value, size_t valueRow) {
    static_cast<ColumnVector<CellView>*>(result)->getRaw()[resultRow] = CellView::nullElement();
}

template <typename Element>
void caseWriteOwnNullCell(Column* result, size_t resultRow, const Column* value, size_t valueRow) {
    std::vector<Element>& results = static_cast<ColumnVector<Element>*>(result)->getRaw();
    results[resultRow] = static_cast<const ColumnVector<Element>*>(value)->getRaw()[valueRow];
}

void writeCell(Column* result,
               size_t resultRow,
               const NLCellWrite& write,
               const Column* value,
               size_t valueRow,
               NLStepBuffers* stepBuffers,
               NLValueStore& values) {
    if (write._write) {
        write._write(result, resultRow, value, valueRow);
        return;
    }

    ListBuffer<>::ListItemVariant item = write._taggedRead(value, valueRow, stepBuffers);
    static_cast<ColumnVector<CellView>*>(result)->getRaw()[resultRow] = values.ownCell(item);
}

template <typename Element>
void fillNullRows(Column* result, const Column* value, const ColumnVector<CustomBool>* nullRows, const Column* fill) {
    const std::vector<Element>& values = static_cast<const ColumnVector<Element>*>(value)->getRaw();
    const std::vector<CustomBool>& nulls = nullRows->getRaw();
    const std::vector<Element>& fills = static_cast<const ColumnVector<Element>*>(fill)->getRaw();
    std::vector<Element>& results = static_cast<ColumnVector<Element>*>(result)->getRaw();

    bioassert(nulls.size() == values.size(), "A value's null rows are marked over other rows than its own.");

    results.resize(values.size());

    const auto isNull = [](CustomBool row) {
        return static_cast<bool>(row);
    };

    size_t row = 0;
    auto fillIt = fills.begin();
    auto resultIt = results.begin();

    while (row < values.size()) {
        const size_t nullStart = static_cast<size_t>(std::find_if(nulls.begin() + row, nulls.end(), isNull) - nulls.begin());
        resultIt = std::copy(values.begin() + row, values.begin() + nullStart, resultIt);

        const size_t nullEnd = static_cast<size_t>(std::find_if_not(nulls.begin() + nullStart, nulls.end(), isNull) - nulls.begin());
        const size_t nullCount = nullEnd - nullStart;
        const size_t fillsLeft = static_cast<size_t>(fills.end() - fillIt);
        bioassert(nullCount <= fillsLeft, "A value has more null rows than the rows filling them.");

        resultIt = std::copy_n(fillIt, nullCount, resultIt);
        fillIt += nullCount;
        row = nullEnd;
    }

    bioassert(fillIt == fills.end(), "A value has fewer null rows than the rows filling them.");
}

template <typename Element>
void fillNullOptRows(Column* result, const Column* value, const ColumnVector<CustomBool>* nullRows, const Column* fill) {
    const ColumnOptVector<Element>* values = static_cast<const ColumnOptVector<Element>*>(value);
    const std::vector<CustomBool>& nulls = nullRows->getRaw();
    const ColumnOptVector<Element>* fills = static_cast<const ColumnOptVector<Element>*>(fill);
    ColumnOptVector<Element>* results = static_cast<ColumnOptVector<Element>*>(result);

    const size_t rowCount = values->size();
    const size_t fillCount = fills->size();
    bioassert(nulls.size() == rowCount, "A value's null rows are marked over other rows than its own.");

    results->resize(rowCount);

    size_t fillRow = 0;
    for (size_t row = 0; row < rowCount; row++) {
        if (nulls[row]) {
            bioassert(fillRow < fillCount, "A value has more null rows than the rows filling them.");
            results->setOptional(row, fills->get(fillRow));
            fillRow++;
        } else {
            results->setOptional(row, values->get(row));
        }
    }

    bioassert(fillRow == fillCount, "A value has fewer null rows than the rows filling them.");
}

// Folds each row's list into the accumulator, the body running once per element position
// over the rows whose list reaches it. @param testStep runs after each chunk of rows a step
// folded has its accumulators written, and hands back the rows whose fold ends there.
template <typename TestStep>
void foldElements(NLExecutionContext* context, NLFoldData* fold, TestStep testStep) {
    const Column* source = fold->getSource();
    const NLUnwindElementCountFunction elementCount = fold->getElementCountFunc();
    const NLUnwindElementEmitFunction elementEmit = fold->getElementEmitFunc();
    const NLCellAbsentFunction cellAbsent = fold->getCellAbsentFunc();
    const Column* initialValue = fold->getInitialValue();
    const Column* value = fold->getValue();
    const NLStmtContainer* body = fold->getStmts();
    Column* accumulator = fold->getAccumulator();
    NLStepBuffers* const memory = fold->getStepBuffers();
    memory->clear();
    NLCompactingValueStore& accumulatorValues = fold->accumulatorValues();
    const NLReownFunction reown = fold->getReown();
    const NLReownRowsFunction reownRows = fold->getReownRows();

    const size_t sourceRows = source->size();
    const size_t chunkSize = context->getChunkSize();

    const auto write = [&](const NLCellWrite& rowWrite, size_t row, const Column* written, size_t writtenRow) {
        writeCell(accumulator, row, rowWrite, written, writtenRow, memory, accumulatorValues.getActive());
    };

    accumulatorValues.clear();
    fold->getReset()(accumulator, sourceRows);

    std::vector<size_t>& elementCounts = fold->elementCounts();
    std::vector<size_t>& reachingRows = fold->reachingRows();
    std::vector<size_t>& endedRows = fold->endedRows();
    elementCounts.assign(sourceRows, 0);
    reachingRows.clear();

    // A row whose cell holds no list keeps the null the reset left it, as Cypher reads
    // `reduce(s = 0, x IN null | s + x)`
    for (size_t row = 0; row < sourceRows; row++) {
        if (cellAbsent(source, row)) {
            continue;
        }

        write(fold->getInitialWrite(), row, initialValue, row);

        elementCounts[row] = elementCount(source, row);
        if (elementCounts[row] > 0) {
            reachingRows.push_back(row);
        }
    }

    std::vector<size_t>& rowsRaw = fold->getRows()->getRaw();
    std::vector<size_t>& positionsRaw = fold->getPositions()->getRaw();

    for (size_t position = 0; !reachingRows.empty(); position++) {
        for (size_t first = 0; first < reachingRows.size(); first += chunkSize) {
            const size_t last = std::min(first + chunkSize, reachingRows.size());
            rowsRaw.assign(reachingRows.begin() + first, reachingRows.begin() + last);

            if (elementEmit) {
                positionsRaw.assign(rowsRaw.size(), position);
                elementEmit(source, fold->getRows(), fold->getPositions(), fold->getElementOutput());
            }

            for (const NLCarriedColumn& carriedColumn : fold->carriedColumns()) {
                const auto gatherFunc = carriedColumn.getGatherFunc();
                gatherFunc(carriedColumn.getInput(), fold->getRows(), carriedColumn.getOutput());
            }

            runBody(context, body);

            bioassert(value->size() == rowsRaw.size(),
                      "Yielded accumulator of a fold is not row-aligned with its elements.");

            for (size_t index = 0; index < rowsRaw.size(); index++) {
                write(fold->getValueWrite(), rowsRaw[index], value, index);
            }

            if (reownRows) {
                reownRows(accumulator, rowsRaw, accumulatorValues.getActive());
            }

            endedRows.clear();
            testStep(endedRows);

            for (const size_t row : endedRows) {
                elementCounts[row] = position + 1;
            }
        }

        std::erase_if(reachingRows, [&](size_t row) { return elementCounts[row] == position + 1; });

        if (reown && !reachingRows.empty() && accumulatorValues.needsCompaction(sourceRows)) {
            reown(accumulator, accumulatorValues.getSpare());
            accumulatorValues.flip();
        }
    }
}

void resetTaggedCells(Column* result, size_t rowCount) {
    std::vector<CellView>& results = static_cast<ColumnVector<CellView>*>(result)->getRaw();
    results.assign(rowCount, CellView::nullElement());
}

template <typename Primitive>
void caseResetColumn(Column* result, size_t rowCount) {
    std::vector<std::optional<Primitive>>& results = static_cast<ColumnOptVector<Primitive>*>(result)->getRaw();
    results.assign(rowCount, std::optional<Primitive> {});
}

// A default-constructed ID is the invalid one and a default-constructed path the one with
// no node, which is how those columns spell an absent row: the reset leaves every row null,
// as the optional one above does
template <typename Element>
void caseResetOwnNullColumn(Column* result, size_t rowCount) {
    std::vector<Element>& results = static_cast<ColumnVector<Element>*>(result)->getRaw();
    results.assign(rowCount, Element {});
}

// 3-way compare two rows of a type-erased column of tagged scalars. Cells need not share
// a type, so a pair of different types compares by the order those types sort in; nulls
// tie and sort after every value, as they do in a nullable value column.
int compareCellColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<CellView>*>(column)->getRaw();
    const std::strong_ordering order = raw[a] <=> raw[b];

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a list or a map column. Two lists order lexicographically
// on the element order above, and two maps by size, then keys, then values, so either can
// be the key the rows are sorted on.
template <typename View>
int compareContainerColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<View>*>(column)->getRaw();
    const std::strong_ordering order = raw[a] <=> raw[b];

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a path column. The empty path a null is spelled as sorts after
// every path, as compareOptColumn places a null after every value.
int comparePathColumn(const Column* column, size_t a, size_t b) {
    const std::vector<GraphPath>& raw = static_cast<const ColumnVector<GraphPath>*>(column)->getRaw();
    const GraphPath& pathA = raw[a];
    const GraphPath& pathB = raw[b];

    const bool aNull = pathA.isNull();
    const bool bNull = pathB.isNull();
    if (aNull || bNull) {
        if (aNull && bNull) {
            return 0;
        }

        return aNull ? 1 : -1;
    }

    const std::strong_ordering order = PathView {pathA.getIDs()} <=> PathView {pathB.getIDs()};

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

// The nullable sibling, for a list or a map read out of a property: a row holding none
// sorts after every value, as compareOptColumn places a null after every value.
template <typename View>
int compareOptContainerColumn(const Column* column, size_t a, size_t b) {
    const ColumnOptVector<View>* typedColumn = static_cast<const ColumnOptVector<View>*>(column);

    const bool aNull = !typedColumn->isValid(a);
    const bool bNull = !typedColumn->isValid(b);
    if (aNull || bNull) {
        if (aNull && bNull) {
            return 0;
        }

        return aNull ? 1 : -1;
    }

    const std::strong_ordering order = typedColumn->getValue(a) <=> typedColumn->getValue(b);

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

// Append the raw bytes of a present property value to a distinct row key. The value
// is appended verbatim: a trivially-copyable primitive copies its object bytes; a
// string copies a length prefix then its characters, so two rows never collide by
// concatenation (so "a"+"b" and "ab"+"" get distinct keys). A view - a list, a map - would
// key on its own bytes, a pointer into its buffer, so it keys through its own serializer.
template <typename Primitive>
    requires (!NLViewElement<Primitive>)
void distinctAppendValueBytes(NLSerializedKey& key, const Primitive& value) {
    key.append(reinterpret_cast<const char*>(&value), sizeof(Primitive));
}

// A float keys by its Cypher value, not its bit pattern: -0.0 is +0.0 (0.0 == -0.0 in
// Cypher) and every NaN payload maps to one canonical NaN, so two distinct bit patterns of
// the same value never split into separate groups (or survive DISTINCT as two rows).
template <std::floating_point Float>
Float normalizedKeyFloat(const Float value) {
    if (value == 0) {
        return 0;
    } else if (std::isnan(value)) {
        return std::numeric_limits<Float>::quiet_NaN();
    }

    return value;
}

template <std::floating_point Float>
void distinctAppendFloatBytes(NLSerializedKey& key, const Float value) {
    const Float normalized = normalizedKeyFloat(value);
    key.append(reinterpret_cast<const char*>(&normalized), sizeof(normalized));
}

void distinctAppendValueBytes(NLSerializedKey& key, double value) {
    distinctAppendFloatBytes(key, value);
}

void distinctAppendValueBytes(NLSerializedKey& key, std::string_view value) {
    const size_t length = value.size();
    key.append(reinterpret_cast<const char*>(&length), sizeof(length));
    key.append(value.data(), value.size());
}

void distinctAppendValueBytes(NLSerializedKey& key, const types::Embedding::Primitive value) {
    const size_t length = value.size();
    key.append(reinterpret_cast<const char*>(&length), sizeof(length));

    for (const float element : value) {
        distinctAppendFloatBytes(key, element);
    }
}

// Serialize one row of an ID column (node/edge/edge-type IDs) into the row key as
// the ID's underlying integer value, byte for byte.
template <typename ElementType>
void distinctKeyAppendColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    const auto value = raw[row].getValue();
    distinctAppendValueBytes(key, value);
}

// Serialize one row of a chunk that holds its values plainly - a column a procedure
// yielded, a tally, an expression over one - into the row key. The ID sibling reads
// through the ID's integer; here the element is the value, and no row of such a chunk is
// null, so the key carries no tag byte to tell a null from a value.
template <typename ElementType>
void distinctKeyAppendPlainColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    distinctAppendValueBytes(key, raw[row]);
}

void distinctAppendListBytes(NLSerializedKey& key, const ListView list);
void distinctAppendMapBytes(NLSerializedKey& key, const MapView map);

template <typename Primitive>
void distinctAppendPresentValue(NLSerializedKey& key, const Primitive& value) {
    key.push_back('\1');
    distinctAppendValueBytes(key, value);
}

void distinctAppendPresentValue(NLSerializedKey& key, const ListView list) {
    distinctAppendListBytes(key, list);
}

void distinctAppendPresentValue(NLSerializedKey& key, const MapView map) {
    distinctAppendMapBytes(key, map);
}

// Serialize one row of a nullable value column into the row key as a tag byte telling
// null from present, then, when present, the value's bytes. A null serializes to the
// tag alone, so all nulls share a key and DISTINCT dedups them together, matching Cypher.
template <typename Primitive>
void distinctKeyAppendOptColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);

    if (!typedColumn->isValid(row)) {
        key.push_back('\0');
        return;
    }

    distinctAppendPresentValue(key, typedColumn->getValue(row));
}

// What a key value packs as, holding the value as its serialized key does: an ID's integer, a
// number's own bits, a double's once normalized, a boolean's byte, the microseconds of a
// datetime or a duration, and a point's own bytes
template <IntegralType T, int I>
T packedKeyValue(ID<T, I> id) {
    return id.getValue();
}

types::Int64::Primitive packedKeyValue(types::Int64::Primitive value) {
    return value;
}

types::UInt64::Primitive packedKeyValue(types::UInt64::Primitive value) {
    return value;
}

uint64_t packedKeyValue(types::Double::Primitive value) {
    return std::bit_cast<uint64_t>(normalizedKeyFloat(value));
}

uint8_t packedKeyValue(types::Bool::Primitive value) {
    return value._boolean;
}

uint8_t packedKeyValue(ValueType value) {
    return static_cast<uint8_t>(value);
}

int64_t packedKeyValue(types::DateTime::Primitive value) {
    return value.getMicroseconds();
}

int64_t packedKeyValue(types::Duration::Primitive value) {
    return value.getMicroseconds();
}

types::Point::Primitive packedKeyValue(types::Point::Primitive value) {
    return value;
}

template <typename ElementType>
void packKeyColumn(const Column* column, size_t beginRow, size_t endRow, const NLKeyPackTarget& target) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    std::byte* value = target._values;

    for (size_t row = beginRow; row < endRow; row++) {
        const auto packed = packedKeyValue(raw[row]);
        std::memcpy(value, &packed, sizeof(packed));

        value += target._valueStride;
    }
}

template <typename Primitive>
void packOptKeyColumn(const Column* column, size_t beginRow, size_t endRow, const NLKeyPackTarget& target) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);
    const std::byte nullMask {static_cast<uint8_t>(1u << (target._nullBit % 8))};

    std::byte* value = target._values;
    std::byte* nulls = target._nulls + target._nullBit / 8;

    for (size_t row = beginRow; row < endRow; row++) {
        if (typedColumn->isValid(row)) {
            const auto packed = packedKeyValue(typedColumn->getValue(row));
            std::memcpy(value, &packed, sizeof(packed));
        } else {
            *nulls |= nullMask;
        }

        value += target._valueStride;
        nulls += target._nullStride;
    }
}

template <typename ElementType>
NLKeyPacking keyPacking() {
    using Packed = decltype(packedKeyValue(std::declval<ElementType>()));

    return {&packKeyColumn<ElementType>, sizeof(Packed)};
}

template <typename Primitive>
NLKeyPacking optKeyPacking() {
    using Packed = decltype(packedKeyValue(std::declval<Primitive>()));

    return {&packOptKeyColumn<Primitive>, sizeof(Packed)};
}

void distinctAppendElementBytes(NLSerializedKey& key, CellView element);

// The count fixes how many IDs follow it, so they need no separator.
void distinctAppendPathBytes(NLSerializedKey& key, std::span<const uint64_t> ids) {
    const size_t idCount = ids.size();
    key.append(reinterpret_cast<const char*>(&idCount), sizeof(idCount));
    key.append(reinterpret_cast<const char*>(ids.data()), ids.size_bytes());
}

// Serialize a nested list into the row key as its length then its elements, so a list
// never keys the same as the concatenation of its neighbours.
void distinctAppendListBytes(NLSerializedKey& key, const ListView list) {
    const size_t size = list.size();
    key.append(reinterpret_cast<const char*>(&size), sizeof(size));

    for (const CellView element : list) {
        distinctAppendElementBytes(key, element);
    }
}

// Serialize a number by its value rather than by the type it is tagged with: an integer
// and a float holding the same value are one Cypher value, so a float with no fractional
// part keys as that integer and only a fractional one keys as a double.
// A number keys under the Int tag whenever it holds one, so 1, 1.0 and a small unsigned 1
// all key alike - which is what makes the key agree with numeric equality.
void distinctAppendIntegerKey(NLSerializedKey& key, const types::Int64::Primitive value) {
    key.push_back(static_cast<char>(CellTag::Int));
    distinctAppendValueBytes(key, value);
}

void distinctAppendNumberKey(NLSerializedKey& key, const types::Int64::Primitive value) {
    distinctAppendIntegerKey(key, value);
}

void distinctAppendNumberKey(NLSerializedKey& key, const types::UInt64::Primitive value) {
    if (value <= static_cast<types::UInt64::Primitive>(std::numeric_limits<types::Int64::Primitive>::max())) {
        distinctAppendIntegerKey(key, static_cast<types::Int64::Primitive>(value));
        return;
    }

    key.push_back(static_cast<char>(CellTag::UInt));
    distinctAppendValueBytes(key, value);
}

void distinctAppendNumberKey(NLSerializedKey& key, const types::Double::Primitive value) {
    constexpr types::Double::Primitive integerBound = 9223372036854775808.0;
    constexpr types::Double::Primitive unsignedBound = 18446744073709551616.0;

    const bool holdsAWholeNumber = std::isfinite(value) && std::trunc(value) == value;
    const bool holdsAnInteger = holdsAWholeNumber && value >= -integerBound && value < integerBound;
    const bool holdsAnUnsigned = holdsAWholeNumber && value >= integerBound && value < unsignedBound;

    if (holdsAnInteger) {
        distinctAppendIntegerKey(key, static_cast<types::Int64::Primitive>(value));
    } else if (holdsAnUnsigned) {
        distinctAppendNumberKey(key, static_cast<types::UInt64::Primitive>(value));
    } else {
        key.push_back(static_cast<char>(CellTag::Double));
        distinctAppendValueBytes(key, value);
    }
}

void distinctAppendNumberBytes(NLSerializedKey& key, const CellView element) {
    switch (element.getTag()) {
        case CellTag::Int:
            return distinctAppendNumberKey(key, element.getAs<types::Int64::Primitive>());
        break;

        case CellTag::UInt:
            return distinctAppendNumberKey(key, element.getAs<types::UInt64::Primitive>());
        break;

        case CellTag::Double:
            return distinctAppendNumberKey(key, element.getAs<types::Double::Primitive>());
        break;

        default:
            bioassert(false, "Keying a non-numeric element as a number");
        break;
    }
}

// Serialize a map into the row key as its size, then each entry's key length, key bytes and
// value. Both producers store entries in key order, so equal maps serialize alike.
void distinctAppendMapBytes(NLSerializedKey& key, const MapView map) {
    const size_t size = map.size();
    key.append(reinterpret_cast<const char*>(&size), sizeof(size));

    for (const MapEntryView entry : map) {
        const std::string_view entryKey = entry.getKey();
        const size_t keySize = entryKey.size();
        key.append(reinterpret_cast<const char*>(&keySize), sizeof(keySize));
        key.append(entryKey);

        distinctAppendElementBytes(key, entry.getValue());
    }
}

// Serialize one tagged scalar into the row key: its tag, then its value's bytes. Cells of
// two types never collide because the tag leads, and a null is the tag alone, so all
// nulls dedup together as they do in a nullable value column.
void distinctAppendElementBytes(NLSerializedKey& key, const CellView element) {
    const CellTag tag = element.getTag();

    switch (tag) {
        case CellTag::Int:
        case CellTag::UInt:
        case CellTag::Double:
            return distinctAppendNumberBytes(key, element);
        break;

        case CellTag::Bool:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, static_cast<bool>(element.getAs<types::Bool::Primitive>()));
            return;
        break;

        case CellTag::String:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::String::Primitive>());
            return;
        break;

        case CellTag::ListView:
            key.push_back(static_cast<char>(tag));
            distinctAppendListBytes(key, element.getAs<ListView>());
            return;
        break;

        case CellTag::Null:
            key.push_back(static_cast<char>(tag));
            return;
        break;

        case CellTag::NodeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<NodeID>().getValue());
            return;
        break;

        case CellTag::EdgeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<EdgeID>().getValue());
            return;
        break;

        case CellTag::DateTime:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::DateTime::Primitive>().getMicroseconds());
            return;
        break;

        case CellTag::Duration:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::Duration::Primitive>().getMicroseconds());
            return;
        break;

        case CellTag::Point:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::Point::Primitive>());
            return;
        break;

        case CellTag::LabelID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<LabelID>().getValue());
            return;
        break;

        case CellTag::EdgeTypeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<EdgeTypeID>().getValue());
            return;
        break;

        case CellTag::PropertyTypeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<PropertyTypeID>().getValue());
            return;
        break;

        case CellTag::ValueType:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<ValueType>());
            return;
        break;

        case CellTag::Embedding:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::Embedding::Primitive>());
            return;
        break;

        case CellTag::MapView:
            key.push_back(static_cast<char>(tag));
            distinctAppendMapBytes(key, element.getAs<MapView>());
            return;
        break;

        case CellTag::Path:
            key.push_back(static_cast<char>(tag));
            distinctAppendPathBytes(key, element.getAs<PathView>().getIDs());
            return;
        break;

        case CellTag::INVALID:
            throw IRException("cannot dedup by an untagged element");
        break;
    }

    bioassert(false, "Unknown CellTag");
}

template <typename ElementType>
bool keyValuesEqual(const ElementType& probe, const ElementType& build) {
    return probe == build;
}

bool keyValuesEqual(types::Embedding::Primitive probe, types::Embedding::Primitive build) {
    return EmbeddingEqual {}(probe, build);
}

// == is equivalence on these, under which two nulls agree, so the join asks `=` itself
template <typename ElementType>
    requires std::same_as<ElementType, ListView>
          || std::same_as<ElementType, MapView>
          || std::same_as<ElementType, CellView>
bool keyValuesEqual(const ElementType& probe, const ElementType& build) {
    const std::optional<CustomBool> equal = Eq {}(probe, build);

    return equal.has_value() && equal->_boolean;
}

// A key no other key equals is one that does not equal itself: a null, a NaN, a list or
// map holding a null at any depth
template <typename ElementType>
bool keyValueIsMatchable(const ElementType& value) {
    return keyValuesEqual(value, value);
}

template <typename ElementType>
bool columnRowMatchable(const Column* column, size_t row) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();

    return keyValueIsMatchable(raw[row]);
}

template <typename Primitive>
bool optColumnRowMatchable(const Column* column, size_t row) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);

    return typedColumn->isValid(row) && keyValueIsMatchable(typedColumn->getValue(row));
}

// Serialize one row of a type-erased column of tagged scalars into the row key.
void distinctKeyAppendCellColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<CellView>*>(column)->getRaw();
    distinctAppendElementBytes(key, raw[row]);
}

// Serialize one row of a list column into the row key, so two rows key alike when their
// lists hold equal elements - the equality Cypher gives two lists.
void distinctKeyAppendListColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ListView>*>(column)->getRaw();
    distinctAppendListBytes(key, raw[row]);
}

void distinctKeyAppendMapColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<MapView>*>(column)->getRaw();
    distinctAppendMapBytes(key, raw[row]);
}

void distinctKeyAppendPathColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const std::vector<GraphPath>& raw = static_cast<const ColumnVector<GraphPath>*>(column)->getRaw();
    const GraphPath& path = raw[row];

    if (path.isNull()) {
        key.push_back(static_cast<char>(CellTag::Null));
        return;
    }

    key.push_back(static_cast<char>(CellTag::Path));
    distinctAppendPathBytes(key, path.getIDs());
}

void distinctKeyAppendOptListColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const ColumnOptVector<ListView>* typedColumn = static_cast<const ColumnOptVector<ListView>*>(column);

    if (!typedColumn->isValid(row)) {
        key.push_back(static_cast<char>(CellTag::Null));
        return;
    }

    distinctAppendPresentValue(key, typedColumn->getValue(row));
}

void distinctKeyAppendOptMapColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const ColumnOptVector<MapView>* typedColumn = static_cast<const ColumnOptVector<MapView>*>(column);

    if (!typedColumn->isValid(row)) {
        key.push_back(static_cast<char>(CellTag::Null));
        return;
    }

    distinctAppendPresentValue(key, typedColumn->getValue(row));
}

template <typename Integral>
    requires std::is_integral_v<Integral> || std::is_enum_v<Integral>
uint64_t hashKeyValue(Integral value) {
    return mixKeyBits(static_cast<uint64_t>(value));
}

template <IntegralType T, int I>
uint64_t hashKeyValue(ID<T, I> id) {
    return hashKeyValue(id.getValue());
}

uint64_t hashKeyValue(CustomBool value) {
    return mixKeyBits(value._boolean);
}

uint64_t hashKeyValue(types::DateTime::Primitive value) {
    return mixKeyBits(static_cast<uint64_t>(value.getMicroseconds()));
}

uint64_t hashKeyValue(types::Duration::Primitive value) {
    return mixKeyBits(static_cast<uint64_t>(value.getMicroseconds()));
}

uint64_t hashKeyValue(types::Point::Primitive value) {
    return mixKeyBits(std::hash<types::Point::Primitive> {}(value));
}

// The two keys of a join need not share a type, so two values `=` holds equal hash alike
// whatever they are held as: a double holding a whole number hashes as that integer. -0.0
// hashes as +0.0, the one value it equals; a NaN never reaches a lookup.
uint64_t hashKeyValue(double value) {
    types::Int64::Primitive integer = 0;
    types::UInt64::Primitive unsignedInteger = 0;

    if (convertNumberExactly(value, integer)) {
        return hashKeyValue(integer);
    } else if (convertNumberExactly(value, unsignedInteger)) {
        return hashKeyValue(unsignedInteger);
    }

    return mixKeyBits(std::bit_cast<uint64_t>(value));
}

uint64_t hashKeyValue(std::string_view value) {
    return std::hash<std::string_view> {}(value);
}

uint64_t hashKeyValue(MapView map) {
    return mixKeyBits(hashMap(map));
}

uint64_t hashKeyValue(types::Embedding::Primitive embedding) {
    uint64_t hash = mixKeyBits(embedding.size());

    for (const float value : embedding) {
        const float normalized = (value == 0.0f) ? 0.0f : value;
        hash = mixKeyBits(hash ^ std::bit_cast<uint32_t>(normalized));
    }

    return hash;
}

uint64_t hashKeyValue(ListView list) {
    return mixKeyBits(hashList(list));
}

uint64_t hashKeyValue(PathView path) {
    const std::span<const uint64_t> ids = path.getIDs();
    uint64_t hash = mixKeyBits(ids.size());

    for (const uint64_t id : ids) {
        hash = mixKeyBits(hash ^ id);
    }

    return hash;
}

// A tagged cell hashes as its value does in a column of that value's type
uint64_t hashKeyValue(CellView cell) {
    switch (cell.getTag()) {
        case CellTag::Int:
            return hashKeyValue(cell.getAs<types::Int64::Primitive>());
        break;

        case CellTag::UInt:
            return hashKeyValue(cell.getAs<types::UInt64::Primitive>());
        break;

        case CellTag::Double:
            return hashKeyValue(cell.getAs<types::Double::Primitive>());
        break;

        case CellTag::Bool:
            return hashKeyValue(cell.getAs<types::Bool::Primitive>());
        break;

        case CellTag::String:
            return hashKeyValue(cell.getAs<types::String::Primitive>());
        break;

        case CellTag::Embedding:
            return hashKeyValue(cell.getAs<types::Embedding::Primitive>());
        break;

        case CellTag::ListView:
            return hashKeyValue(cell.getAs<ListView>());
        break;

        case CellTag::MapView:
            return hashKeyValue(cell.getAs<MapView>());
        break;

        case CellTag::NodeID:
            return hashKeyValue(cell.getAs<NodeID>());
        break;

        case CellTag::EdgeID:
            return hashKeyValue(cell.getAs<EdgeID>());
        break;

        case CellTag::DateTime:
            return hashKeyValue(cell.getAs<types::DateTime::Primitive>());
        break;

        case CellTag::Duration:
            return hashKeyValue(cell.getAs<types::Duration::Primitive>());
        break;

        case CellTag::Point:
            return hashKeyValue(cell.getAs<types::Point::Primitive>());
        break;

        case CellTag::Path:
            return hashKeyValue(cell.getAs<PathView>());
        break;

        case CellTag::LabelID:
            return hashKeyValue(cell.getAs<LabelID>());
        break;

        case CellTag::EdgeTypeID:
            return hashKeyValue(cell.getAs<EdgeTypeID>());
        break;

        case CellTag::PropertyTypeID:
            return hashKeyValue(cell.getAs<PropertyTypeID>());
        break;

        case CellTag::ValueType:
            return hashKeyValue(cell.getAs<ValueType>());
        break;

        case CellTag::Null:
        case CellTag::INVALID:
            return 0;
        break;
    }

    bioassert(false, "Unknown CellTag");
    return 0;
}

template <typename ElementType>
void hashKeyColumn(const Column* column, std::vector<uint64_t>& hashes) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    hashes.resize(raw.size());

    std::transform(raw.begin(), raw.end(), hashes.begin(), [](const ElementType& value) {
        return hashKeyValue(value);
    });
}

template <typename Primitive>
void hashOptKeyColumn(const Column* column, std::vector<uint64_t>& hashes) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);

    const size_t rowCount = typedColumn->size();
    hashes.resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        hashes[row] = typedColumn->isValid(row) ? hashKeyValue(typedColumn->getValue(row)) : uint64_t {0};
    }
}

template <typename ElementType>
bool keyEqualColumn(const Column* probe, size_t probeRow, const Column* build, size_t buildRow) {
    const auto& probeRaw = static_cast<const ColumnVector<ElementType>*>(probe)->getRaw();
    const auto& buildRaw = static_cast<const ColumnVector<ElementType>*>(build)->getRaw();

    return keyValuesEqual(probeRaw[probeRow], buildRaw[buildRow]);
}

template <typename Primitive>
bool keyEqualOptColumn(const Column* probe, size_t probeRow, const Column* build, size_t buildRow) {
    const ColumnOptVector<Primitive>* typedProbe = static_cast<const ColumnOptVector<Primitive>*>(probe);
    const ColumnOptVector<Primitive>* typedBuild = static_cast<const ColumnOptVector<Primitive>*>(build);

    return keyValuesEqual(typedProbe->getValue(probeRow), typedBuild->getValue(buildRow));
}

template <typename ElementType>
NLJoinKeyFunctions joinKeyFunctions() {
    return {&hashKeyColumn<ElementType>, &keyEqualColumn<ElementType>, &columnRowMatchable<ElementType>};
}

template <typename Primitive>
NLJoinKeyFunctions optJoinKeyFunctions() {
    return {&hashOptKeyColumn<Primitive>, &keyEqualOptColumn<Primitive>, &optColumnRowMatchable<Primitive>};
}

template <typename ColumnType>
concept RowKeyColumn = requires(const ColumnType* column) { operandRow(column, size_t {0}); };

// A probe key and a build key of two types, compared by the `=` of the filter the join
// replaces: an unknown answer matches nothing, as the filter kept only the true rows
template <typename ProbeColumn, typename BuildColumn>
bool keyEqualAcrossTypes(const Column* probe, size_t probeRow, const Column* build, size_t buildRow) {
    const ProbeColumn* typedProbe = static_cast<const ProbeColumn*>(probe);
    const BuildColumn* typedBuild = static_cast<const BuildColumn*>(build);

    const auto equal = Eq {}(operandRow(typedProbe, probeRow), operandRow(typedBuild, buildRow));
    if constexpr (TypeUtils::is_optional_v<std::decay_t<decltype(equal)>>) {
        return equal.has_value() && equal->_boolean;
    } else {
        return static_cast<bool>(equal);
    }
}

struct CrossTypeKeyEqualSelector {
    NLKeyEqualFunction _equal {nullptr};

    template <typename ProbeColumn, typename BuildColumn>
    void operator()(const ProbeColumn*, const BuildColumn*) {
        if constexpr (RowKeyColumn<ProbeColumn> && RowKeyColumn<BuildColumn>) {
            _equal = &keyEqualAcrossTypes<ProbeColumn, BuildColumn>;
        }
    }
};

// The group of build rows carrying a key, or noRow when no indexed row carries it. One
// bucket chains the groups of every key that falls in it, so a group's key is confirmed
// by hash and then by value.
size_t findKeyGroup(const NLHashJoinIndex& index,
                    uint64_t hash,
                    const Column* probeKey,
                    size_t probeRow,
                    const Column* buildKey,
                    NLKeyEqualFunction keyEqual) {
    for (size_t group = index.getFirstGroup(hash); group != NLHashJoinIndex::noRow; group = index.getNextGroup(group)) {
        const bool sameHash = index.getHash(group) == hash;

        if (sameHash && keyEqual(probeKey, probeRow, buildKey, group)) {
            return group;
        }
    }

    return NLHashJoinIndex::noRow;
}

// Count the non-null cells of a type-erased column of tagged scalars, so count(x) over a
// heterogeneous unwind charges the same rows a nullable value column would.
size_t countNonNullElementsColumn(const Column* column) {
    const auto& raw = static_cast<const ColumnVector<CellView>*>(column)->getRaw();
    return std::count_if(raw.begin(), raw.end(), [](const CellView element) {
        return element.getTag() != CellTag::Null;
    });
}

// An entity an OPTIONAL MATCH did not match is an invalid ID, which is a null, so an ID
// chunk charges only the rows holding a valid one - Cypher's count(x) counts the non-null
// rows. The ID sibling of countPresentColumn, for the one column family whose null is not
// a missing optional.
template <typename ID>
size_t countValidIDs(const Column* column) {
    const std::vector<ID>& raw = static_cast<const ColumnVector<ID>*>(column)->getRaw();

    return std::ranges::count_if(raw, [](const ID id) { return id.isValid(); });
}

size_t countPresentPaths(const Column* column) {
    const std::vector<GraphPath>& raw = static_cast<const ColumnVector<GraphPath>*>(column)->getRaw();

    return std::ranges::count_if(raw, [](const GraphPath& path) { return !path.isNull(); });
}

// Count the present (non-null) values of a nullable value column, so Cypher count(x)
// charges only the rows in which x is not null.
template <typename Primitive>
size_t countPresentColumn(const Column* column) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);
    return typedColumn->size() - typedColumn->getNullCount();
}

// Add two aggregate values with defined overflow. A signed integer sum wraps in
// two's complement (matching Cypher's Java-long integer sum) rather than invoking
// C++ signed-overflow undefined behavior; unsigned integers already wrap by the
// language, and doubles use the built-in +.
template <typename Primitive>
Primitive numericAdd(Primitive accumulator, Primitive value) {
    if constexpr (std::is_integral_v<Primitive> && std::is_signed_v<Primitive>) {
        using Unsigned = std::make_unsigned_t<Primitive>;
        return static_cast<Primitive>(static_cast<Unsigned>(accumulator) + static_cast<Unsigned>(value));
    } else {
        return accumulator + value;
    }
}

// A number sums as an f64 and a point keeps its running mean. A duration's total is the
// state's 128-bit one.
template <typename Primitive>
using AverageTotal = std::conditional_t<std::is_same_v<Primitive, types::Duration::Primitive>
                                            || std::is_same_v<Primitive, types::Point::Primitive>,
                                        Primitive,
                                        types::Double::Primitive>;

// @param count is how many values @param total already holds, which a running mean needs
template <typename Primitive>
AverageTotal<Primitive> addToAverageTotal(AverageTotal<Primitive> total, Primitive value, uint64_t count) {
    if constexpr (std::is_same_v<Primitive, types::Point::Primitive>) {
        return Point::addToMean(total, count, value);
    } else {
        return total + static_cast<types::Double::Primitive>(value);
    }
}

types::Double::Primitive averageOf(types::Double::Primitive total, uint64_t count) {
    return total / static_cast<types::Double::Primitive>(count);
}

// The mean of durations is within their range, so it is a duration. A mean finer than a
// microsecond truncates toward zero, as a duration divided by an integer does.
types::Duration::Primitive averageOf(__int128 total, uint64_t count) {
    return types::Duration::Primitive(static_cast<int64_t>(total / static_cast<__int128>(count)));
}

// A sum of durations is a 128-bit count of microseconds until it is emitted, so a total
// that passes the range of a duration and comes back answers in every row order
types::Duration::Primitive durationOfTotal(__int128 total) {
    const bool outsideTheRange = total > std::numeric_limits<int64_t>::max()
                              || total < std::numeric_limits<int64_t>::min();

    if (outsideTheRange) {
        throw IRException("Duration overflow");
    }

    return types::Duration::Primitive(static_cast<int64_t>(total));
}

types::Point::Primitive averageOf(types::Point::Primitive mean, uint64_t count) {
    return mean;
}

template <typename Select>
void dispatchAverageTotal(ValueType totalType, Select&& select) {
    switch (totalType) {
        case ValueType::Double:
            select.template operator()<types::Double::Primitive>();
        break;

        case ValueType::Duration:
            select.template operator()<types::Duration::Primitive>();
        break;

        case ValueType::Point:
            select.template operator()<types::Point::Primitive>();
        break;

        default:
            throw IRException("avg answers a double, a duration or a point");
        break;
    }
}

std::optional<CellView> presentCell(const CellView element) {
    if (element.getTag() == CellTag::Null) {
        return std::nullopt;
    }

    return element;
}

std::optional<CellView> presentRow(const ColumnVector<CellView>* column, size_t row) {
    return presentCell(column->getRaw()[row]);
}

template <typename Primitive>
std::optional<Primitive> presentRow(const ColumnOptVector<Primitive>* column, size_t row) {
    return operandRow(column, row);
}

template <typename Primitive, bool IsMax>
bool replacesExtremum(const Primitive& current, const Primitive& value) {
    if constexpr (IsMax) {
        return sortsBefore(current, value);
    } else {
        return sortsBefore(value, current);
    }
}

// The number a tagged cell holds, whatever numeric type its tag names. A deviation over
// a type-erased column is defined over numbers only, so any other tag is a query error -
// the check the static column types make for a typed column, made per row here.
double taggedNumericValue(const CellView element) {
    const CellTag tag = element.getTag();

    switch (tag) {
        case CellTag::Int:
            return static_cast<double>(element.getAs<types::Int64::Primitive>());
        break;

        case CellTag::UInt:
            return static_cast<double>(element.getAs<types::UInt64::Primitive>());
        break;

        case CellTag::Double:
            return element.getAs<types::Double::Primitive>();
        break;

        default:
            throw IRException(fmt::format("stDev() and stDevP() read numbers, not '{}'", queryTypeName(tag)));
        break;
    }
}

// A standard deviation keeps the mean of the values folded so far and the sum of their
// squared deviations from it. Welford's update keeps both accurate for values far from 0,
// where a sum of squares minus a squared sum cancels to noise. The count keeps the two
// doubles apart: side by side, GCC packs them in one register and serializes the update.
struct NLDeviationState {
    double _mean {0.0};
    uint64_t _count {0};
    double _squaredDeviations {0.0};
};

template <typename Primitive>
double numberOf(const Primitive& value) {
    return static_cast<double>(value);
}

double numberOf(const CellView element) {
    return taggedNumericValue(element);
}

void foldDeviation(double value, double& mean, double& squaredDeviations, uint64_t& count) {
    count++;

    const double distanceToOldMean = value - mean;
    mean += distanceToOldMean / static_cast<double>(count);
    squaredDeviations += distanceToOldMean * (value - mean);
}

// Too few values to spread - none, or the one a sample has no second for - spread by 0
template <bool Population>
double deviation(double squaredDeviations, uint64_t count) {
    if constexpr (Population) {
        if (count == 0) {
            return 0.0;
        } else {
            return std::sqrt(squaredDeviations / static_cast<double>(count));
        }
    } else {
        if (count < 2) {
            return 0.0;
        } else {
            return std::sqrt(squaredDeviations / static_cast<double>(count - 1));
        }
    }
}

// stDev(DISTINCT x) moves a group's state once per distinct value it sees
template <typename InputColumn, bool Distinct>
void groupFoldDeviation(NLGroupStatePlaces places,
                        const Column* input,
                        const std::vector<size_t>& groups,
                        size_t beginRow,
                        size_t endRow,
                        const NLGroupDistinctTally* distinct,
                        NLValueStore& values) {
    const InputColumn* typedInput = static_cast<const InputColumn*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        const auto value = presentRow(typedInput, row);
        if (!value.has_value()) {
            continue;
        }

        const size_t group = groups[row];

        if constexpr (Distinct) {
            if (!distinct->isNew(row)) {
                continue;
            }
        }

        NLDeviationState& state = places.getState<NLDeviationState>(group);
        foldDeviation(numberOf(*value), state._mean, state._squaredDeviations, state._count);
    }
}

// The fold of a standard deviation over a column of this value type: like avg's, a
// numeric column only
template <bool Distinct>
NLGroupAggregateFoldFunction selectGroupDeviationFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldDeviation<ColumnOptVector<types::Int64::Primitive>, Distinct>;
        break;

        case ValueType::UInt64:
            return &groupFoldDeviation<ColumnOptVector<types::UInt64::Primitive>, Distinct>;
        break;

        case ValueType::Double:
            return &groupFoldDeviation<ColumnOptVector<types::Double::Primitive>, Distinct>;
        break;

        default:
            throw IRException("stdev requires a numeric column");
        break;
    }

    return nullptr;
}

constexpr std::byte INTEGER_ZERO_CELL[sizeof(CellTag) + sizeof(types::Int64::Primitive)] {
    static_cast<std::byte>(CellTag::Int),
};

// The running sum of tagged cells, as Cypher adds them: an integer while every number
// summed is one, a float from the first float on, a duration over durations, added into a
// 128-bit total of microseconds, and the running mean of the points an average reads. A null
// sum has summed nothing yet. Integers wrap as a typed column's sum does.
class NLTaggedSum {
public:
    void add(CellView value) {
        const CellTag tag = value.getTag();

        switch (tag) {
            case CellTag::Int:
                addInteger(tag, value.getAs<types::Int64::Primitive>());
            break;

            case CellTag::UInt:
                addInteger(tag, static_cast<types::Int64::Primitive>(value.getAs<types::UInt64::Primitive>()));
            break;

            case CellTag::Double:
                addReal(tag, value.getAs<types::Double::Primitive>());
            break;

            case CellTag::Duration:
                addDuration(value.getAs<types::Duration::Primitive>());
            break;

            default:
                throw IRException(fmt::format("sum() and avg() read numbers or durations, not '{}'", queryTypeName(tag)));
            break;
        }
    }

    // An average reads every number as a float, as it does over a typed column. @param count
    // is how many cells the average already holds, which the running mean of points needs.
    void addToAverage(CellView value, uint64_t count) {
        const CellTag tag = value.getTag();
        const bool isNumber = tag == CellTag::Int || tag == CellTag::UInt || tag == CellTag::Double;

        if (isNumber) {
            addReal(tag, taggedNumericValue(value));
        } else if (tag == CellTag::Point) {
            addPoint(value.getAs<types::Point::Primitive>(), count);
        } else {
            add(value);
        }
    }

    CellView sum(QueryListBuffer& cells) const {
        if (_tag == CellTag::Null) {
            return CellView {INTEGER_ZERO_CELL};
        } else if (_tag == CellTag::Duration) {
            return stagedCell(&cells, durationOfTotal(_durationTotal));
        } else if (_tag == CellTag::Double) {
            return stagedCell(&cells, _real);
        }

        return stagedCell(&cells, _integer);
    }

    CellView average(uint64_t count, QueryListBuffer& cells) const {
        if (_tag == CellTag::Null) {
            return CellView::nullElement();
        } else if (_tag == CellTag::Duration) {
            return stagedCell(&cells, averageOf(_durationTotal, count));
        } else if (_tag == CellTag::Point) {
            return stagedCell(&cells, _point);
        }

        bioassert(_tag == CellTag::Double, "An average sums its numbers as floats");

        return stagedCell(&cells, averageOf(_real, count));
    }

private:
    CellTag _tag {CellTag::Null};
    types::Int64::Primitive _integer {0};
    types::Double::Primitive _real {0.0};
    __int128 _durationTotal {0};
    types::Point::Primitive _point {};

    // Numbers sum together, and a duration or a point only with its own kind
    static CellTag sumKind(CellTag tag) {
        const bool sumsAlone = tag == CellTag::Duration || tag == CellTag::Point;
        return sumsAlone ? tag : CellTag::Double;
    }

    void throwIfMixing(CellTag tag) const {
        const bool summedAny = _tag != CellTag::Null;
        const bool mixes = sumKind(_tag) != sumKind(tag);

        if (summedAny && mixes) {
            throw IRException(fmt::format("sum() and avg() cannot add '{}' to a sum of '{}'",
                                          queryTypeName(tag),
                                          queryTypeName(_tag)));
        }
    }

    void addInteger(CellTag tag, types::Int64::Primitive value) {
        throwIfMixing(tag);

        if (_tag == CellTag::Double) {
            _real += static_cast<types::Double::Primitive>(value);
        } else {
            _tag = CellTag::Int;
            _integer = numericAdd(_integer, value);
        }
    }

    void addReal(CellTag tag, types::Double::Primitive value) {
        throwIfMixing(tag);

        if (_tag != CellTag::Double) {
            _tag = CellTag::Double;
            _real = static_cast<types::Double::Primitive>(_integer);
        }

        _real += value;
    }

    void addDuration(types::Duration::Primitive value) {
        throwIfMixing(CellTag::Duration);

        _tag = CellTag::Duration;
        _durationTotal += value.getMicroseconds();
    }

    void addPoint(types::Point::Primitive value, uint64_t count) {
        throwIfMixing(CellTag::Point);

        _tag = CellTag::Point;
        _point = Point::addToMean(_point, count, value);
    }
};

// Append the values of the given rows of an input chunk onto the tail of a key
// buffer of the same element type. nl.group_aggregate_update passes the rows that
// created a new group this step, so the buffer grows one key value per group in
// creation order.
template <typename ElementType>
void groupGatherAppendColumn(const Column* input,
                             const std::vector<size_t>& rows,
                             Column* buffer,
                             NLValueStore& values) {
    const auto& inputRaw = static_cast<const ColumnVector<ElementType>*>(input)->getRaw();
    auto& bufferRaw = static_cast<ColumnVector<ElementType>*>(buffer)->getRaw();

    for (const size_t row : rows) {
        bufferRaw.push_back(values.ownElement(inputRaw[row]));
    }
}

template <typename Element>
void groupGatherAppendOptColumn(const Column* input,
                                const std::vector<size_t>& rows,
                                Column* buffer,
                                NLValueStore& values) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedBuffer = static_cast<ColumnOptVector<Element>*>(buffer);

    for (const size_t row : rows) {
        if (typedInput->isValid(row)) {
            typedBuffer->push_back(values.ownElement(typedInput->getValue(row)));
        } else {
            typedBuffer->pushNull();
        }
    }
}

template <typename ColumnType>
void reownColumn(Column* column, NLValueStore& values) {
    values.ownRows(static_cast<ColumnType*>(column), 0);
}

template <typename ColumnType>
void reownColumnRows(Column* column, const std::vector<size_t>& rows, NLValueStore& values) {
    ColumnType* typedColumn = static_cast<ColumnType*>(column);

    for (const size_t row : rows) {
        values.ownRow(typedColumn, row);
    }
}

// Construct the identity of a state - a zero tally, sum or spread, no value kept yet - in the
// blocks of new groups
template <typename State>
void groupInitStates(NLGroupStatePlaces places, size_t begin, size_t count) {
    for (size_t group = begin; group < begin + count; group++) {
        new (places.getPlace(group)) State {};
    }
}

// The state type of an aggregate each group keeps a State for, finalized by emit
template <typename State>
void describeGroupState(NLGroupAggregateEmitFunction emit, NLGroupAggregateStateType& stateType) {
    static_assert(std::is_trivially_copyable_v<State>, "The blocks move a state as bytes and never destroy it");
    static_assert(alignof(State) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__, "The blocks are as aligned as operator new makes them");

    stateType._size = sizeof(State);
    stateType._alignment = alignof(State);
    stateType._init = &groupInitStates<State>;
    stateType._emit = emit;
    stateType._reown = nullptr;
}

// The value a group keeps - its min, max or first - null until the group sees one. A string,
// list or map keeps the store's copy of what it views.
template <typename Primitive>
class NLKeptValue {
public:
    bool isKept() const { return _value.has_value(); }
    const Primitive& getValue() const { return *_value; }

    void keep(const Primitive& value, NLValueStore& values) { _value = values.ownElement(value); }
    void reown(NLValueStore& values) {
        if (isKept()) {
            keep(*_value, values);
        }
    }

private:
    std::optional<Primitive> _value;
};

// A type-erased cell is kept inline, as a list holds it: its tag, then its value's bytes. A
// number, a boolean, an ID, a datetime or a duration is kept without a copy into the store; a
// string, list, map, embedding or path keeps the store's copy of what its bytes view.
template <>
class NLKeptValue<CellView> {
public:
    bool isKept() const { return _bytes[0] != NOTHING_KEPT; }

    // The kept cell, viewed where this state holds it
    CellView getValue() const { return CellView {_bytes}; }

    void keep(CellView cell, NLValueStore& values) {
        const CellTag tag = cell.getTag();

        CellTagDispatcher {tag}.execute([&]<typename T>(CellView view) {
            static_assert(sizeof(T) <= VALUE_BYTES, "A kept cell holds the value of every tag inline");

            T value = view.getAs<T>();
            if constexpr (NLViewElement<T>) {
                value = values.own(value);
            }

            _bytes[0] = std::byte {static_cast<uint8_t>(tag)};
            std::memcpy(_bytes + 1, &value, sizeof(T));
        }, cell);
    }

    void reown(NLValueStore& values) {
        if (isKept()) {
            keep(getValue(), values);
        }
    }

private:
    static constexpr size_t VALUE_BYTES = sizeof(types::Point::Primitive);
    static constexpr std::byte NOTHING_KEPT {static_cast<uint8_t>(CellTag::INVALID)};

    std::byte _bytes[1 + VALUE_BYTES] {NOTHING_KEPT};
};

template <typename Primitive>
void groupReownKeptValues(NLGroupStatePlaces places, size_t groupCount, NLValueStore& values) {
    for (size_t group = 0; group < groupCount; group++) {
        places.getState<NLKeptValue<Primitive>>(group).reown(values);
    }
}

// Emit the value a group keeps, null for a group that kept none
template <typename Primitive>
void groupEmitKeptValue(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers);

// The state type of a min, max or first
template <typename Primitive>
void describeGroupKeptValueState(NLGroupAggregateStateType& stateType) {
    describeGroupState<NLKeptValue<Primitive>>(&groupEmitKeptValue<Primitive>, stateType);

    if constexpr (NLViewElement<Primitive>) {
        stateType._reown = &groupReownKeptValues<Primitive>;
    }
}

// The state of an average: the total of the values folded and how many there were. A group
// that folded none averages to null.
template <typename Total>
struct NLAverageState {
    Total _total {};
    uint64_t _count {0};
};

template <typename State>
State& placedState(std::byte* place) {
    return *std::launder(reinterpret_cast<State*>(place));
}

// Fold a chunk's present values into a keyless sum
template <typename Primitive>
void aggregateFoldSum(std::byte* place, const Column* input, NLCompactingValueStore& values) {
    Primitive& sum = placedState<Primitive>(place);
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    Primitive running = sum;
    const size_t rowCount = typedInput->size();
    for (size_t row = 0; row < rowCount; row++) {
        if (typedInput->isValid(row)) {
            running = numericAdd(running, typedInput->getValue(row));
        }
    }

    sum = running;
}

// Fold a chunk's present values into a keyless average: a total widened to f64, or the running
// mean of points, and the count
template <typename Primitive>
void aggregateFoldAvg(std::byte* place, const Column* input, NLCompactingValueStore& values) {
    using State = NLAverageState<AverageTotal<Primitive>>;

    State& average = placedState<State>(place);
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    AverageTotal<Primitive> total = average._total;
    uint64_t count = average._count;

    const size_t rowCount = typedInput->size();
    for (size_t row = 0; row < rowCount; row++) {
        if (typedInput->isValid(row)) {
            total = addToAverageTotal(total, typedInput->getValue(row), count);
            count++;
        }
    }

    average._total = total;
    average._count = count;
}

// Fold a chunk's present durations into the 128-bit total of a keyless sum, or of an average
// beside its count
template <bool Averages>
void aggregateFoldDurationTotal(std::byte* place, const Column* input, NLCompactingValueStore& values) {
    const ColumnOptVector<types::Duration::Primitive>* typedInput =
        static_cast<const ColumnOptVector<types::Duration::Primitive>*>(input);

    __int128 total = 0;
    uint64_t count = 0;
    const size_t rowCount = typedInput->size();
    for (size_t row = 0; row < rowCount; row++) {
        if (typedInput->isValid(row)) {
            total += typedInput->getValue(row).getMicroseconds();
            count++;
        }
    }

    if constexpr (Averages) {
        NLAverageState<__int128>& average = placedState<NLAverageState<__int128>>(place);
        average._total += total;
        average._count += count;
    } else {
        placedState<__int128>(place) += total;
    }
}

template <typename InputColumn>
void aggregateFoldDeviation(std::byte* place, const Column* input, NLCompactingValueStore& values) {
    NLDeviationState& state = placedState<NLDeviationState>(place);
    const InputColumn* typedInput = static_cast<const InputColumn*>(input);

    uint64_t count = state._count;
    double mean = state._mean;
    double squaredDeviations = state._squaredDeviations;

    const size_t rowCount = typedInput->size();
    for (size_t row = 0; row < rowCount; row++) {
        const auto value = presentRow(typedInput, row);
        if (value.has_value()) {
            foldDeviation(numberOf(*value), mean, squaredDeviations, count);
        }
    }

    state._count = count;
    state._mean = mean;
    state._squaredDeviations = squaredDeviations;
}

// Fold a chunk's present values into a keyless min (IsMax false) or max (IsMax true). The
// chunk's extreme is found first, then kept once if it replaces the one kept before, so the
// store holds one copy of a view rather than one per row.
template <typename Primitive, bool IsMax, typename InputColumn = ColumnOptVector<Primitive>>
void aggregateFoldMinMax(std::byte* place, const Column* input, NLCompactingValueStore& values) {
    NLKeptValue<Primitive>& kept = placedState<NLKeptValue<Primitive>>(place);
    const InputColumn* typedInput = static_cast<const InputColumn*>(input);

    std::optional<Primitive> extreme;
    if (kept.isKept()) {
        extreme = kept.getValue();
    }

    bool replaced = false;
    const size_t rowCount = typedInput->size();
    for (size_t row = 0; row < rowCount; row++) {
        const std::optional<Primitive> value = presentRow(typedInput, row);
        if (!value.has_value()) {
            continue;
        }

        if (!extreme.has_value() || replacesExtremum<Primitive, IsMax>(*extreme, *value)) {
            extreme = *value;
            replaced = true;
        }
    }

    if (replaced) {
        kept.keep(*extreme, values.getSpare());
        values.flip();
    }
}

// Fold a chunk of tagged cells into a keyless sum, or an average beside its count. A cell tagged
// null is skipped, as a null value column row is.
template <bool Averages>
void aggregateFoldTaggedSum(std::byte* place, const Column* input, NLCompactingValueStore& values) {
    using State = std::conditional_t<Averages, NLAverageState<NLTaggedSum>, NLTaggedSum>;

    State& state = placedState<State>(place);
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    State running = state;
    for (const CellView& cell : inputRaw) {
        const std::optional<CellView> value = presentCell(cell);
        if (!value) {
            continue;
        }

        if constexpr (Averages) {
            running._total.addToAverage(*value, running._count);
            running._count++;
        } else {
            running.add(*value);
        }
    }

    state = running;
}

// The fold of a keyless sum over a column of this value type. sum adds the values, so only a
// numeric or a duration column is valid.
NLAggregateFoldFunction selectSumFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &aggregateFoldSum<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &aggregateFoldSum<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &aggregateFoldSum<types::Double::Primitive>;
        break;

        case ValueType::Duration:
            return &aggregateFoldDurationTotal</*Averages=*/false>;
        break;

        default:
            throw IRException("sum requires a numeric or a duration column");
        break;
    }

    return nullptr;
}

NLAggregateFoldFunction selectAvgFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &aggregateFoldAvg<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &aggregateFoldAvg<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &aggregateFoldAvg<types::Double::Primitive>;
        break;

        case ValueType::Duration:
            return &aggregateFoldDurationTotal</*Averages=*/true>;
        break;

        case ValueType::Point:
            return &aggregateFoldAvg<types::Point::Primitive>;
        break;

        default:
            throw IRException("avg requires a numeric, a duration or a point column");
        break;
    }

    return nullptr;
}

NLAggregateFoldFunction selectDeviationFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &aggregateFoldDeviation<ColumnOptVector<types::Int64::Primitive>>;
        break;

        case ValueType::UInt64:
            return &aggregateFoldDeviation<ColumnOptVector<types::UInt64::Primitive>>;
        break;

        case ValueType::Double:
            return &aggregateFoldDeviation<ColumnOptVector<types::Double::Primitive>>;
        break;

        default:
            throw IRException("stdev requires a numeric column");
        break;
    }

    return nullptr;
}

template <bool IsMax>
NLAggregateFoldFunction selectMinMaxFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &aggregateFoldMinMax<types::Int64::Primitive, IsMax>;
        break;

        case ValueType::UInt64:
            return &aggregateFoldMinMax<types::UInt64::Primitive, IsMax>;
        break;

        case ValueType::Double:
            return &aggregateFoldMinMax<types::Double::Primitive, IsMax>;
        break;

        case ValueType::Bool:
            return &aggregateFoldMinMax<types::Bool::Primitive, IsMax>;
        break;

        case ValueType::String:
            return &aggregateFoldMinMax<types::String::Primitive, IsMax>;
        break;

        case ValueType::DateTime:
            return &aggregateFoldMinMax<types::DateTime::Primitive, IsMax>;
        break;

        case ValueType::Duration:
            return &aggregateFoldMinMax<types::Duration::Primitive, IsMax>;
        break;

        case ValueType::Point:
            return &aggregateFoldMinMax<types::Point::Primitive, IsMax>;
        break;

        case ValueType::List:
            return &aggregateFoldMinMax<types::List::Primitive, IsMax>;
        break;

        case ValueType::Map:
            return &aggregateFoldMinMax<types::Map::Primitive, IsMax>;
        break;

        default:
            throw IRException("min/max requires an orderable column");
        break;
    }

    return nullptr;
}

NLAggregateFoldFunction taggedAggregateFoldFor(AggregateKind kind) {
    switch (kind) {
        case AggregateKind::Sum:
            return &aggregateFoldTaggedSum</*Averages=*/false>;
        break;

        case AggregateKind::Avg:
            return &aggregateFoldTaggedSum</*Averages=*/true>;
        break;

        case AggregateKind::StDev:
        case AggregateKind::StDevP:
            return &aggregateFoldDeviation<ColumnVector<CellView>>;
        break;

        case AggregateKind::Min:
            return &aggregateFoldMinMax<CellView, /*IsMax=*/false, ColumnVector<CellView>>;
        break;

        case AggregateKind::Max:
            return &aggregateFoldMinMax<CellView, /*IsMax=*/true, ColumnVector<CellView>>;
        break;
    }

    bioassert(false, "Unhandled aggregate kind");
    return nullptr;
}

// Fold a chunk's present values into per-group sums. Each new group starts at zero, so the
// value is added into its running sum.
template <typename Primitive>
void groupFoldSum(NLGroupStatePlaces places,
                  const Column* input,
                  const std::vector<size_t>& groups,
                  size_t beginRow,
                  size_t endRow,
                  const NLGroupDistinctTally* distinct,
                  NLValueStore& values) {
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        if (typedInput->isValid(row)) {
            Primitive& running = places.getState<Primitive>(groups[row]);
            running = numericAdd(running, typedInput->getValue(row));
        }
    }
}

// Fold a chunk's distinct present values into per-group sums (sum(DISTINCT x)): a value its
// group has already been charged is skipped, so each distinct value is added once however
// many rows carry it.
template <typename Primitive>
void groupFoldSumDistinct(NLGroupStatePlaces places,
                          const Column* input,
                          const std::vector<size_t>& groups,
                          size_t beginRow,
                          size_t endRow,
                          const NLGroupDistinctTally* distinct,
                          NLValueStore& values) {
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        if (!typedInput->isValid(row)) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        Primitive& running = places.getState<Primitive>(group);
        running = numericAdd(running, typedInput->getValue(row));
    }
}

// Fold a chunk's present durations into per-group 128-bit totals, once per distinct value
// of a group for sum(DISTINCT x) and avg(DISTINCT x), tallying the ones an average folds
template <bool Distinct, bool Averages>
void groupFoldDurationTotal(NLGroupStatePlaces places,
                            const Column* input,
                            const std::vector<size_t>& groups,
                            size_t beginRow,
                            size_t endRow,
                            const NLGroupDistinctTally* distinct,
                            NLValueStore& values) {
    const ColumnOptVector<types::Duration::Primitive>* typedInput =
        static_cast<const ColumnOptVector<types::Duration::Primitive>*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        if (!typedInput->isValid(row)) {
            continue;
        }

        const size_t group = groups[row];

        if constexpr (Distinct) {
            if (!distinct->isNew(row)) {
                continue;
            }
        }

        const __int128 microseconds = typedInput->getValue(row).getMicroseconds();

        if constexpr (Averages) {
            NLAverageState<__int128>& average = places.getState<NLAverageState<__int128>>(group);
            average._total += microseconds;
            average._count++;
        } else {
            places.getState<__int128>(group) += microseconds;
        }
    }
}

// Fold a group's tagged cells into its running sum in place, tallying the ones an average
// folds. The type-erased sibling of groupFoldSum and groupFoldAvg.
template <bool Distinct, bool Averages>
void groupFoldTaggedSum(NLGroupStatePlaces places,
                        const Column* input,
                        const std::vector<size_t>& groups,
                        size_t beginRow,
                        size_t endRow,
                        const NLGroupDistinctTally* distinct,
                        NLValueStore& values) {
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    for (size_t row = beginRow; row < endRow; row++) {
        const std::optional<CellView> value = presentCell(inputRaw[row]);
        if (!value) {
            continue;
        }

        const size_t group = groups[row];

        if constexpr (Distinct) {
            if (!distinct->isNew(row)) {
                continue;
            }
        }

        if constexpr (Averages) {
            NLAverageState<NLTaggedSum>& average = places.getState<NLAverageState<NLTaggedSum>>(group);
            average._total.addToAverage(*value, average._count);
            average._count++;
        } else {
            places.getState<NLTaggedSum>(group).add(*value);
        }
    }
}

// Fold a chunk's present values into per-group min (IsMax false) or max (IsMax
// true) states. The first present value of a group seeds it; a later value
// replaces it when more extreme. A group with no present value stays null.
template <typename Primitive, bool IsMax, typename InputColumn = ColumnOptVector<Primitive>>
void groupFoldMinMax(NLGroupStatePlaces places,
                     const Column* input,
                     const std::vector<size_t>& groups,
                     size_t beginRow,
                     size_t endRow,
                     const NLGroupDistinctTally* distinct,
                     NLValueStore& values) {
    const InputColumn* typedInput = static_cast<const InputColumn*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        const std::optional<Primitive> value = presentRow(typedInput, row);
        if (!value.has_value()) {
            continue;
        }

        NLKeptValue<Primitive>& current = places.getState<NLKeptValue<Primitive>>(groups[row]);
        if (!current.isKept() || replacesExtremum<Primitive, IsMax>(current.getValue(), *value)) {
            current.keep(*value, values);
        }
    }
}

// Fold a chunk's present values into per-group first states: the first present value of a
// group is kept and every later one passed over.
template <typename Primitive, typename InputColumn = ColumnOptVector<Primitive>>
void groupFoldFirst(NLGroupStatePlaces places,
                    const Column* input,
                    const std::vector<size_t>& groups,
                    size_t beginRow,
                    size_t endRow,
                    const NLGroupDistinctTally* distinct,
                    NLValueStore& values) {
    const InputColumn* typedInput = static_cast<const InputColumn*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        NLKeptValue<Primitive>& current = places.getState<NLKeptValue<Primitive>>(groups[row]);
        if (current.isKept()) {
            continue;
        }

        const std::optional<Primitive> value = presentRow(typedInput, row);
        if (value.has_value()) {
            current.keep(*value, values);
        }
    }
}

NLGroupAggregateFoldFunction taggedGroupAggregateFoldFor(GroupAggregateKind kind) {
    switch (kind) {
        case GroupAggregateKind::Sum:
            return &groupFoldTaggedSum</*Distinct=*/false, /*Averages=*/false>;
        break;

        case GroupAggregateKind::SumDistinct:
            return &groupFoldTaggedSum</*Distinct=*/true, /*Averages=*/false>;
        break;

        case GroupAggregateKind::Avg:
            return &groupFoldTaggedSum</*Distinct=*/false, /*Averages=*/true>;
        break;

        case GroupAggregateKind::AvgDistinct:
            return &groupFoldTaggedSum</*Distinct=*/true, /*Averages=*/true>;
        break;

        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevP:
            return &groupFoldDeviation<ColumnVector<CellView>, /*Distinct=*/false>;
        break;

        case GroupAggregateKind::StDevDistinct:
        case GroupAggregateKind::StDevPDistinct:
            return &groupFoldDeviation<ColumnVector<CellView>, /*Distinct=*/true>;
        break;

        case GroupAggregateKind::Min:
            return &groupFoldMinMax<CellView, /*IsMax=*/false, ColumnVector<CellView>>;
        break;

        case GroupAggregateKind::Max:
            return &groupFoldMinMax<CellView, /*IsMax=*/true, ColumnVector<CellView>>;
        break;

        case GroupAggregateKind::First:
            return &groupFoldFirst<CellView, ColumnVector<CellView>>;
        break;

        case GroupAggregateKind::Count:
        case GroupAggregateKind::CountDistinct:
        case GroupAggregateKind::CountRows:
            bioassert(false, "A tally of type-erased cells has its own fold");
        break;
    }

    bioassert(false, "Unhandled group aggregate kind");
    return nullptr;
}

// Fold a chunk's present values into per-group averages: a running total (a number widened
// to f64, or the running mean of points) plus the count of the values folded. avg divides
// the two at the emit step.
template <typename Primitive>
void groupFoldAvg(NLGroupStatePlaces places,
                  const Column* input,
                  const std::vector<size_t>& groups,
                  size_t beginRow,
                  size_t endRow,
                  const NLGroupDistinctTally* distinct,
                  NLValueStore& values) {
    using State = NLAverageState<AverageTotal<Primitive>>;

    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        if (typedInput->isValid(row)) {
            State& average = places.getState<State>(groups[row]);
            average._total = addToAverageTotal(average._total, typedInput->getValue(row), average._count);
            average._count++;
        }
    }
}

// Fold a chunk's distinct present values into per-group averages (avg(DISTINCT x)): a value
// its group has already been charged moves neither the total nor the count, so the mean is
// taken over the distinct values.
template <typename Primitive>
void groupFoldAvgDistinct(NLGroupStatePlaces places,
                          const Column* input,
                          const std::vector<size_t>& groups,
                          size_t beginRow,
                          size_t endRow,
                          const NLGroupDistinctTally* distinct,
                          NLValueStore& values) {
    using State = NLAverageState<AverageTotal<Primitive>>;

    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        if (!typedInput->isValid(row)) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        State& average = places.getState<State>(group);
        average._total = addToAverageTotal(average._total, typedInput->getValue(row), average._count);
        average._count++;
    }
}

// Tally every row into its group (count(*) over a never-null column): each row
// charges its group regardless of value, so the input values are never read.
void groupFoldCountAll(NLGroupStatePlaces places,
                       const Column* input,
                       const std::vector<size_t>& groups,
                       size_t beginRow,
                       size_t endRow,
                       const NLGroupDistinctTally* distinct,
                       NLValueStore& values) {
    for (size_t row = beginRow; row < endRow; row++) {
        places.getState<uint64_t>(groups[row])++;
    }
}

// Tally each group's present (non-null) rows (count(x)): a null row is not charged,
// matching Cypher's count(x).
template <typename Primitive>
void groupFoldCountPresent(NLGroupStatePlaces places,
                           const Column* input,
                           const std::vector<size_t>& groups,
                           size_t beginRow,
                           size_t endRow,
                           const NLGroupDistinctTally* distinct,
                           NLValueStore& values) {
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        if (typedInput->isValid(row)) {
            places.getState<uint64_t>(groups[row])++;
        }
    }
}

// Tally each group's rows holding a valid ID (count(n) over a node/edge column): an
// invalid ID is a null, so it is not charged - the ID sibling of groupFoldCountPresent.
template <typename ID>
void groupFoldCountValidID(NLGroupStatePlaces places,
                           const Column* input,
                           const std::vector<size_t>& groups,
                           size_t beginRow,
                           size_t endRow,
                           const NLGroupDistinctTally* distinct,
                           NLValueStore& values) {
    const std::vector<ID>& inputRaw = static_cast<const ColumnVector<ID>*>(input)->getRaw();

    for (size_t row = beginRow; row < endRow; row++) {
        if (inputRaw[row].isValid()) {
            places.getState<uint64_t>(groups[row])++;
        }
    }
}

void groupFoldCountPresentPath(NLGroupStatePlaces places,
                               const Column* input,
                               const std::vector<size_t>& groups,
                               size_t beginRow,
                               size_t endRow,
                               const NLGroupDistinctTally* distinct,
                               NLValueStore& values) {
    const std::vector<GraphPath>& inputRaw = static_cast<const ColumnVector<GraphPath>*>(input)->getRaw();

    for (size_t row = beginRow; row < endRow; row++) {
        if (!inputRaw[row].isNull()) {
            places.getState<uint64_t>(groups[row])++;
        }
    }
}

// A row of a chunk an expression or a procedure filled is never null. An ID or a path is
// null on the rows an OPTIONAL MATCH missed.
template <typename ElementType>
bool presentChunkRow(const ElementType& value) {
    return true;
}

template <IntegralType T, int tag>
bool presentChunkRow(const ID<T, tag>& id) {
    return id.isValid();
}

bool presentChunkRow(const GraphPath& path) {
    return !path.isNull();
}

// Tally each group's distinct present values of a chunk. The tally keys a row the way a
// whole-row DISTINCT keys it, so two rows count once exactly when DISTINCT would fold
// them together.
template <typename ElementType>
void groupFoldCountDistinctChunk(NLGroupStatePlaces places,
                                 const Column* input,
                                 const std::vector<size_t>& groups,
                                 size_t beginRow,
                                 size_t endRow,
                                 const NLGroupDistinctTally* distinct,
                                 NLValueStore& values) {
    const std::vector<ElementType>& inputRaw = static_cast<const ColumnVector<ElementType>*>(input)->getRaw();

    for (size_t row = beginRow; row < endRow; row++) {
        if (!presentChunkRow(inputRaw[row])) {
            continue;
        }

        const size_t group = groups[row];

        if (distinct->isNew(row)) {
            places.getState<uint64_t>(group)++;
        }
    }
}

// Tally each group's distinct present values (count(DISTINCT x)): a null row is not
// charged, and a value repeated within its group is charged once. The value bytes are
// the DISTINCT serializer's, so two rows count as one exactly when a whole-row DISTINCT
// would fold them together.
template <typename Primitive>
void groupFoldCountDistinctPresent(NLGroupStatePlaces places,
                                   const Column* input,
                                   const std::vector<size_t>& groups,
                                   size_t beginRow,
                                   size_t endRow,
                                   const NLGroupDistinctTally* distinct,
                                   NLValueStore& values) {
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        if (!typedInput->isValid(row)) {
            continue;
        }

        const size_t group = groups[row];

        if (distinct->isNew(row)) {
            places.getState<uint64_t>(group)++;
        }
    }
}

// Tally each group's present cells of a type-erased column of tagged scalars, so a
// grouped count(x) over a heterogeneous unwind charges the same rows a nullable value
// column would.
void groupFoldCountPresentCell(NLGroupStatePlaces places,
                               const Column* input,
                               const std::vector<size_t>& groups,
                               size_t beginRow,
                               size_t endRow,
                               const NLGroupDistinctTally* distinct,
                               NLValueStore& values) {
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    for (size_t row = beginRow; row < endRow; row++) {
        if (presentCell(inputRaw[row])) {
            places.getState<uint64_t>(groups[row])++;
        }
    }
}

// Tally each group's distinct present cells of a type-erased column of tagged scalars.
// The key carries the tag as well as the value, so cells of different types are told
// apart the way a whole-row DISTINCT tells them apart.
void groupFoldCountDistinctCell(NLGroupStatePlaces places,
                                const Column* input,
                                const std::vector<size_t>& groups,
                                size_t beginRow,
                                size_t endRow,
                                const NLGroupDistinctTally* distinct,
                                NLValueStore& values) {
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    for (size_t row = beginRow; row < endRow; row++) {
        const std::optional<CellView> element = presentCell(inputRaw[row]);
        if (!element) {
            continue;
        }

        const size_t group = groups[row];

        if (distinct->isNew(row)) {
            places.getState<uint64_t>(group)++;
        }
    }
}

// Append a chunk's present values to the flat value buffer, recording each element's
// position in its group's list. A null value is skipped (Cypher collect ignores
// nulls). The flat buffer holds the collected type's primitive; the input is its
// nullable value chunk.
template <typename Primitive>
void collectFold(Column* values,
                 const Column* input,
                 const std::vector<size_t>& groups,
                 std::vector<std::vector<size_t>>& groupPositions,
                 const NLGroupDistinctTally* distinct,
                 NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Primitive>*>(values)->getRaw();
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    const size_t rowCount = typedInput->size();
    for (size_t row = 0; row < rowCount; row++) {
        if (typedInput->isValid(row)) {
            const size_t position = valuesRaw.size();
            valuesRaw.push_back(store.ownElement(typedInput->getValue(row)));
            groupPositions[groups[row]].push_back(position);
        }
    }
}

// Append this step's values a group has not collected yet - collect(DISTINCT x). A value
// repeated within its group is dropped rather than buffered a second time, so the list is
// the group's distinct values in first-seen order.
template <typename Primitive>
void collectFoldDistinct(Column* values,
                         const Column* input,
                         const std::vector<size_t>& groups,
                         std::vector<std::vector<size_t>>& groupPositions,
                         const NLGroupDistinctTally* distinct,
                         NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Primitive>*>(values)->getRaw();
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    const size_t rowCount = typedInput->size();
    for (size_t row = 0; row < rowCount; row++) {
        if (!typedInput->isValid(row)) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownElement(typedInput->getValue(row)));
        groupPositions[group].push_back(position);
    }
}

// The cell sibling of collectFold, for a chunk holding a value in every row: every row of
// the input joins its group's list.
template <typename Cell>
void collectCellFold(Column* values,
                     const Column* input,
                     const std::vector<size_t>& groups,
                     std::vector<std::vector<size_t>>& groupPositions,
                     const NLGroupDistinctTally* distinct,
                     NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Cell>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<Cell>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        groupPositions[groups[row]].push_back(valuesRaw.size());
        valuesRaw.push_back(store.ownElement(inputRaw[row]));
    }
}

// The ID sibling of collectFold: an entity an OPTIONAL MATCH did not match is an invalid
// ID, which is how a null entity is spelled, so it is dropped the way collectFold drops an
// absent optional rather than collected as the value 2^64-1.
template <typename IDType>
void collectValidIDFold(Column* values,
                        const Column* input,
                        const std::vector<size_t>& groups,
                        std::vector<std::vector<size_t>>& groupPositions,
                        const NLGroupDistinctTally* distinct,
                        NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<IDType>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<IDType>*>(input)->getRaw();

    const size_t base = valuesRaw.size();
    const auto isValid = [](const IDType id) { return id.isValid(); };

    if (std::ranges::all_of(inputRaw, isValid)) {
        valuesRaw.insert(valuesRaw.end(), inputRaw.begin(), inputRaw.end());

        for (size_t row = 0; row < inputRaw.size(); row++) {
            groupPositions[groups[row]].push_back(base + row);
        }

        return;
    }

    valuesRaw.reserve(base + inputRaw.size());

    for (size_t row = 0; row < inputRaw.size(); row++) {
        if (!inputRaw[row].isValid()) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(inputRaw[row]);
        groupPositions[groups[row]].push_back(position);
    }
}

// The ID sibling of collectFoldDistinct: an entity repeated within its group joins the
// list once, keyed by the ID's underlying integer, and the null an unmatched pattern left
// is no key of its own.
template <typename IDType>
void collectValidIDFoldDistinct(Column* values,
                                const Column* input,
                                const std::vector<size_t>& groups,
                                std::vector<std::vector<size_t>>& groupPositions,
                                const NLGroupDistinctTally* distinct,
                                NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<IDType>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<IDType>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        if (!inputRaw[row].isValid()) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(inputRaw[row]);
        groupPositions[group].push_back(position);
    }
}

// The path sibling of collectValidIDFold: the path an OPTIONAL MATCH left where its pattern
// missed holds no node, which is how a null path is spelled, so it is dropped. A kept path is
// copied into the store as a tagged cell, so its lists emit as a type-erased collect's do.
void collectPathFold(Column* values,
                     const Column* input,
                     const std::vector<size_t>& groups,
                     std::vector<std::vector<size_t>>& groupPositions,
                     const NLGroupDistinctTally* distinct,
                     NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<CellView>*>(values)->getRaw();
    const std::vector<GraphPath>& inputRaw = static_cast<const ColumnVector<GraphPath>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const GraphPath& path = inputRaw[row];
        if (path.isNull()) {
            continue;
        }

        ListBuffer<>::ListItemVariant item {PathView {path.getIDs()}};

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownCell(item));
        groupPositions[groups[row]].push_back(position);
    }
}

void collectPathFoldDistinct(Column* values,
                             const Column* input,
                             const std::vector<size_t>& groups,
                             std::vector<std::vector<size_t>>& groupPositions,
                             const NLGroupDistinctTally* distinct,
                             NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<CellView>*>(values)->getRaw();
    const std::vector<GraphPath>& inputRaw = static_cast<const ColumnVector<GraphPath>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const GraphPath& path = inputRaw[row];
        if (path.isNull()) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        ListBuffer<>::ListItemVariant item {PathView {path.getIDs()}};

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownCell(item));
        groupPositions[group].push_back(position);
    }
}

// The list- or map-buffer value a tagged cell holds, read back as the type its tag names, so
// a collect of type-erased cells buffers each one under the type it came in with.
template <typename Item>
Item taggedItem(const CellView element) {
    const CellTag tag = element.getTag();

    switch (tag) {
        case CellTag::Int:
            return Item {element.getAs<types::Int64::Primitive>()};
        break;

        case CellTag::UInt:
            return Item {element.getAs<types::UInt64::Primitive>()};
        break;

        case CellTag::Double:
            return Item {element.getAs<types::Double::Primitive>()};
        break;

        case CellTag::Bool:
            return Item {element.getAs<types::Bool::Primitive>()};
        break;

        case CellTag::String:
            return Item {element.getAs<types::String::Primitive>()};
        break;

        case CellTag::Embedding:
            return Item {element.getAs<types::Embedding::Primitive>()};
        break;

        case CellTag::ListView:
            return Item {element.getAs<ListView>()};
        break;

        case CellTag::Null:
            return Item {element.getAs<PropertyNull>()};
        break;

        case CellTag::NodeID:
            return Item {element.getAs<NodeID>()};
        break;

        case CellTag::EdgeID:
            return Item {element.getAs<EdgeID>()};
        break;

        case CellTag::DateTime:
            return Item {element.getAs<types::DateTime::Primitive>()};
        break;

        case CellTag::Duration:
            return Item {element.getAs<types::Duration::Primitive>()};
        break;

        case CellTag::Point:
            return Item {element.getAs<types::Point::Primitive>()};
        break;

        case CellTag::MapView:
            return Item {element.getAs<MapView>()};
        break;

        case CellTag::Path:
            return Item {element.getAs<PathView>()};
        break;

        case CellTag::LabelID:
            return Item {element.getAs<LabelID>()};
        break;

        case CellTag::EdgeTypeID:
            return Item {element.getAs<EdgeTypeID>()};
        break;

        case CellTag::PropertyTypeID:
            return Item {element.getAs<PropertyTypeID>()};
        break;

        case CellTag::ValueType:
            return Item {element.getAs<ValueType>()};
        break;

        case CellTag::INVALID:
            throw IRException("cannot collect an untagged element");
        break;
    }

    throw IRException("Unknown CellTag");
}

// The type-erased sibling of collectFold: a tagged cell is there in every row, but one
// tagged null is the null Cypher's collect drops, so only the rest join the group's list.
void collectTaggedFold(Column* values,
                       const Column* input,
                       const std::vector<size_t>& groups,
                       std::vector<std::vector<size_t>>& groupPositions,
                       const NLGroupDistinctTally* distinct,
                       NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<CellView>*>(values)->getRaw();
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<CellView> element = presentCell(inputRaw[row]);
        if (!element) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownElement(*element));
        groupPositions[groups[row]].push_back(position);
    }
}

// The dedup sibling: a tagged cell keys by the value its tag names, so the same number
// reached under two tags keys once - the ordering rules read these cells the same way.
void collectTaggedFoldDistinct(Column* values,
                               const Column* input,
                               const std::vector<size_t>& groups,
                               std::vector<std::vector<size_t>>& groupPositions,
                               const NLGroupDistinctTally* distinct,
                               NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<CellView>*>(values)->getRaw();
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<CellView> element = presentCell(inputRaw[row]);
        if (!element) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownElement(*element));
        groupPositions[group].push_back(position);
    }
}

// The type-erased sibling of collectListEmit: each buffered cell goes into the list
// buffer as the value its own tag names, so the list keeps the types it gathered.
void collectTaggedListEmit(const Column* values,
                           const std::vector<std::vector<size_t>>& groupPositions,
                           size_t begin,
                           size_t count,
                           ListBuffer<>& listBuffer,
                           Column* output) {
    const auto& valuesRaw = static_cast<const ColumnVector<CellView>*>(values)->getRaw();
    auto& outputRaw = static_cast<ColumnVector<ListView>*>(output)->getRaw();

    outputRaw.clear();
    outputRaw.reserve(count);

    std::vector<ListBuffer<>::ListItemVariant> elements;
    for (size_t index = 0; index < count; index++) {
        const std::vector<size_t>& positions = groupPositions[begin + index];

        elements.clear();
        elements.reserve(positions.size());
        for (const size_t position : positions) {
            elements.push_back(taggedItem<ListBuffer<>::ListItemVariant>(valuesRaw[position]));
        }

        outputRaw.push_back(listBuffer.insert(elements));
    }
}

// The list and map sibling of collectValidIDFoldDistinct: two cells are the same value
// when their elements are, so a cell keys by the container serialized element by element
// rather than by its view's own span, which two equal containers never share.
template <typename Container>
void collectContainerFoldDistinct(Column* values,
                                  const Column* input,
                                  const std::vector<size_t>& groups,
                                  std::vector<std::vector<size_t>>& groupPositions,
                                  const NLGroupDistinctTally* distinct,
                                  NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Container>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<Container>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownElement(inputRaw[row]));
        groupPositions[group].push_back(position);
    }
}

// The nullable siblings of collectCellFold and collectContainerFoldDistinct for a list or a
// map chunk: a row an OPTIONAL MATCH left with no container is dropped, as collectFold drops
// an absent optional, so the buffer they fill is a plain container column either way.
template <typename Container>
void collectOptContainerFold(Column* values,
                             const Column* input,
                             const std::vector<size_t>& groups,
                             std::vector<std::vector<size_t>>& groupPositions,
                             const NLGroupDistinctTally* distinct,
                             NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Container>*>(values)->getRaw();
    const ColumnOptVector<Container>* typedInput = static_cast<const ColumnOptVector<Container>*>(input);

    const size_t rowCount = typedInput->size();
    for (size_t row = 0; row < rowCount; row++) {
        if (!typedInput->isValid(row)) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownElement(typedInput->getValue(row)));
        groupPositions[groups[row]].push_back(position);
    }
}

template <typename Container>
void collectOptContainerFoldDistinct(Column* values,
                                     const Column* input,
                                     const std::vector<size_t>& groups,
                                     std::vector<std::vector<size_t>>& groupPositions,
                                     const NLGroupDistinctTally* distinct,
                                     NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Container>*>(values)->getRaw();
    const ColumnOptVector<Container>* typedInput = static_cast<const ColumnOptVector<Container>*>(input);

    const size_t rowCount = typedInput->size();
    for (size_t row = 0; row < rowCount; row++) {
        if (!typedInput->isValid(row)) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownElement(typedInput->getValue(row)));
        groupPositions[group].push_back(position);
    }
}

// Emit a chunk of unwound values (nl.unwind_collect): for each flat-buffer position this chunk
// covers, write the present value into the nullable value output. collect dropped
// nulls, so every emitted element is present.
template <typename Primitive>
void unwindCollectValueEmit(const Column* values,
                     const ColumnVector<size_t>* positions,
                     Column* output) {
    const auto& valuesRaw = static_cast<const ColumnVector<Primitive>*>(values)->getRaw();
    ColumnOptVector<Primitive>* typedOutput = static_cast<ColumnOptVector<Primitive>*>(output);
    const auto& positionsRaw = positions->getRaw();

    typedOutput->clear();
    typedOutput->resize(positionsRaw.size());

    for (size_t row = 0; row < positionsRaw.size(); row++) {
        typedOutput->set(row, valuesRaw[positionsRaw[row]]);
    }
}

// Emit a chunk of per-group lists (nl.collect): for each group in [begin, begin+count),
// gather its elements from the flat buffer into the list buffer as one contiguous run
// and store the resulting ListView in the list output.
template <typename Primitive>
void collectListEmit(const Column* values,
                     const std::vector<std::vector<size_t>>& groupPositions,
                     size_t begin,
                     size_t count,
                     ListBuffer<>& listBuffer,
                     Column* output) {
    const auto& valuesRaw = static_cast<const ColumnVector<Primitive>*>(values)->getRaw();
    auto& outputRaw = static_cast<ColumnVector<ListView>*>(output)->getRaw();

    outputRaw.clear();
    outputRaw.reserve(count);

    std::vector<ListBuffer<>::ListItemVariant> elements;
    for (size_t index = 0; index < count; index++) {
        const std::vector<size_t>& positions = groupPositions[begin + index];

        elements.clear();
        elements.reserve(positions.size());
        for (const size_t position : positions) {
            elements.push_back(ListBuffer<>::ListItemVariant {valuesRaw[position]});
        }

        outputRaw.push_back(listBuffer.insert(elements));
    }
}

// Invoke handler with the element a collect of a nullable chunk of @param kind buffers: a
// scalar, an embedding, a schema ID or a list. A map collects through handlers of its own.
template <typename Handler>
void dispatchCollectedChunkKind(NLChunkKind kind, Handler&& handler) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return handler.template operator()<EdgeTypeID>();
        break;

        case NLChunkKind::LabelID:
            return handler.template operator()<LabelID>();
        break;

        case NLChunkKind::PropertyTypeID:
            return handler.template operator()<PropertyTypeID>();
        break;

        case NLChunkKind::ValueTypeCode:
            return handler.template operator()<ValueType>();
        break;

        case NLChunkKind::Int64:
            return handler.template operator()<types::Int64::Primitive>();
        break;

        case NLChunkKind::UInt64:
            return handler.template operator()<types::UInt64::Primitive>();
        break;

        case NLChunkKind::Double:
            return handler.template operator()<types::Double::Primitive>();
        break;

        case NLChunkKind::Bool:
            return handler.template operator()<types::Bool::Primitive>();
        break;

        case NLChunkKind::String:
            return handler.template operator()<types::String::Primitive>();
        break;

        case NLChunkKind::Embedding:
            return handler.template operator()<types::Embedding::Primitive>();
        break;

        case NLChunkKind::List:
            return handler.template operator()<ListView>();
        break;

        case NLChunkKind::DateTime:
            return handler.template operator()<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return handler.template operator()<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return handler.template operator()<types::Point::Primitive>();
        break;

        case NLChunkKind::Map:
        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::LabelSetID:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("collect does not support this value type");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
}

// Every cell of the column is a list of its own, so none of them is the null a
// comprehension hands its row back.
bool presentCell(const Column*, size_t) {
    return false;
}

bool absentListCell(const Column* source, size_t row) {
    return !(*static_cast<const ColumnOptVector<ListView>*>(source))[row].has_value();
}

template <typename Primitive>
bool absentValueCell(const Column* source, size_t row) {
    return !(*static_cast<const ColumnOptVector<Primitive>*>(source))[row].has_value();
}

// A cell of a type-erased column carries its own tag, so the null is the one that tag
// names rather than a cell the column does not hold.
bool absentTaggedCell(const Column* source, size_t row) {
    const CellView& element = (*static_cast<const ColumnVector<CellView>*>(source))[row];

    return !iteratedList(element);
}

// Read one cell of a nullable value column as the value it contributes to a list or a map:
// the value it holds, or the tagged null Cypher leaves where the row has none.
template <typename Item, typename Primitive>
Item valueItem(const Column* input, size_t row, NLStepBuffers*) {
    const std::optional<Primitive>& cell = (*static_cast<const ColumnOptVector<Primitive>*>(input))[row];
    if (!cell.has_value()) {
        return Item {PropertyNull {}};
    }

    return Item {*cell};
}

// Read one cell of a nullable integer column as a bound of a range: the number it holds,
// or nothing where the row has none.
std::optional<ListView> plainListRead(const Column* input, size_t row) {
    return (*static_cast<const ColumnVector<ListView>*>(input))[row];
}

std::optional<ListView> optListRead(const Column* input, size_t row) {
    return (*static_cast<const ColumnOptVector<ListView>*>(input))[row];
}

std::optional<ListView> constListRead(const Column* input, size_t) {
    return static_cast<const ColumnConst<ListView>*>(input)->getRaw();
}

std::optional<ListView> optConstListRead(const Column* input, size_t) {
    return static_cast<const ColumnConst<std::optional<ListView>>*>(input)->getRaw();
}

// The map sibling of the list reads above: one cell of a map column, or nothing where the
// row holds no map.
template <typename ColumnT>
NLKeyedValue mapRead(const Column* input, size_t row) {
    const std::optional<MapView> map = (*static_cast<const ColumnT*>(input))[row];
    if (!map) {
        return std::monostate {};
    }

    return *map;
}

NLKeyedValue readKeyedCell(const CellView cell) {
    const CellTag tag = cell.getTag();

    switch (tag) {
        case CellTag::MapView:
            return cell.getAs<MapView>();
        break;
        case CellTag::NodeID:
            return cell.getAs<NodeID>();
        break;
        case CellTag::EdgeID:
            return cell.getAs<EdgeID>();
        break;
        case CellTag::DateTime:
            return cell.getAs<DateTime>();
        break;
        case CellTag::Duration:
            return cell.getAs<Duration>();
        break;
        case CellTag::Point:
            return cell.getAs<Point>();
        break;
        case CellTag::Null:
            return std::monostate {};
        break;
        case CellTag::Int:
        case CellTag::UInt:
        case CellTag::Double:
        case CellTag::Bool:
        case CellTag::String:
        case CellTag::Embedding:
        case CellTag::ListView:
        case CellTag::Path:
        case CellTag::LabelID:
        case CellTag::EdgeTypeID:
        case CellTag::PropertyTypeID:
        case CellTag::ValueType:
            return NLKeylessValue {tag};
        break;
        case CellTag::INVALID:
            throw IRException("a map key read reads an untagged cell");
        break;
    }

    throw IRException("Unknown CellTag");
}

// One cell of a column of tagged cells, read as what its own tag says it holds: a map, a
// node or an edge read out of a list or out of another map arrives as such a column
template <typename ColumnT>
NLKeyedValue readTaggedKeyed(const Column* input, size_t row) {
    return readKeyedCell((*static_cast<const ColumnT*>(input))[row]);
}

template <typename Held>
CellTag heldTag(const Held& held) {
    const auto tagOf = []<typename Value>(const Value& value) {
        if constexpr (std::is_same_v<Value, std::monostate>) {
            return CellTag::Null;
        } else if constexpr (std::is_same_v<Value, NLKeylessValue> || std::is_same_v<Value, NLNeitherPositionNorKey>) {
            return value._tag;
        } else {
            return TypeToCellTag<Value>::Tag;
        }
    };

    return std::visit(tagOf, held);
}

// The entry @param keyed holds under @param key: the one its map holds, or its node's or
// edge's property, written into @param mapBuffer as a map of that one entry. False where
// it holds null or no such entry, and raises where it holds a value with no entries.
bool findKeyedEntry(const NLKeyedValue& keyed,
                    std::string_view key,
                    NLEntityProperties& properties,
                    MapBuffer<>& mapBuffer,
                    MapEntryView& entry) {
    MapBuffer<>::MapKeyValuePair property {.key = key};

    if (std::holds_alternative<std::monostate>(keyed)) {
        return false;
    } else if (const MapView* map = std::get_if<MapView>(&keyed)) {
        return findMapEntry(*map, key, entry);
    } else if (const NodeID* node = std::get_if<NodeID>(&keyed)) {
        if (!properties.readValue(*node, key, property.value)) {
            return false;
        }
    } else if (const EdgeID* edge = std::get_if<EdgeID>(&keyed)) {
        if (!properties.readValue(*edge, key, property.value)) {
            return false;
        }
    } else {
        throwIndexTypeError(heldTag(keyed), CellTag::String);
    }

    entry = mapBuffer.insert(std::span<const MapBuffer<>::MapKeyValuePair> {&property, 1}).front();
    return true;
}

MapBuffer<>::MapItemVariant mapItemOf(int64_t component) {
    return component;
}

MapBuffer<>::MapItemVariant mapItemOf(const Point::Component& component) {
    return std::visit([](auto value) -> MapBuffer<>::MapItemVariant { return value; }, component);
}

// The component @param part names of @param value, written into @param mapBuffer as the
// value of a map of one entry, as an entity's property is
template <typename Owner, typename Part>
CellView componentCell(Owner value,
                       std::optional<Part> part,
                       std::string_view key,
                       std::string_view ownerName,
                       MapBuffer<>& mapBuffer) {
    if (!part) {
        throw IRException(fmt::format("'{}' is not a component of a {}", key, ownerName));
    }

    const MapBuffer<>::MapKeyValuePair component {
        .key = key,
        .value = mapItemOf(Owner::component(value, *part)),
    };

    const MapView map = mapBuffer.insert(std::span<const MapBuffer<>::MapKeyValuePair> {&component, 1});

    return map.front().getValue();
}

// The value a property read finds in @param keyed: a key of its map, a property of its node
// or edge, or a component of its instant, duration or point. Null where it holds null or no
// such key.
CellView readStaticKey(const NLKeyedValue& keyed,
                       const NLStaticMapKeyData* mapKey,
                       NLEntityProperties& properties,
                       MapBuffer<>& mapBuffer) {
    const std::string_view key = mapKey->getKey();

    if (const DateTime* instant = std::get_if<DateTime>(&keyed)) {
        return componentCell(*instant, mapKey->getPart<DateTimePart>(), key, "datetime", mapBuffer);
    } else if (const Duration* duration = std::get_if<Duration>(&keyed)) {
        return componentCell(*duration, mapKey->getPart<DurationPart>(), key, "duration", mapBuffer);
    } else if (const Point* point = std::get_if<Point>(&keyed)) {
        return componentCell(*point, mapKey->getPart<PointPart>(), key, "point", mapBuffer);
    } else if (const NLKeylessValue* keyless = std::get_if<NLKeylessValue>(&keyed)) {
        throw IRException(fmt::format("Type mismatch: a value of type '{}' has no property '{}'",
                                      queryTypeName(keyless->_tag),
                                      key));
    }

    MapEntryView entry;
    if (!findKeyedEntry(keyed, key, properties, mapBuffer, entry)) {
        return CellView::nullElement();
    }

    return entry.getValue();
}

// The key one column of a dynamic map key read holds at @param row, or nothing where the
// row holds none.
template <typename ColumnT>
std::optional<types::String::Primitive> stringRead(const Column* input, size_t row) {
    return (*static_cast<const ColumnT*>(input))[row];
}

template <typename ColumnT>
std::optional<types::String::Primitive> stringValueArgument(const Column* input, size_t row, std::string_view) {
    return (*static_cast<const ColumnT*>(input))[row];
}

template <typename ColumnT>
std::optional<types::String::Primitive> cellStringArgument(const Column* input, size_t row, std::string_view functionName) {
    return cellString((*static_cast<const ColumnT*>(input))[row], functionName);
}

bool listStrings(const ListView list, std::string_view functionName, std::vector<types::String::Primitive>& strings) {
    for (const CellView element : list) {
        const std::optional<types::String::Primitive> string = cellString(element, functionName);
        if (!string.has_value()) {
            return false;
        }

        strings.push_back(*string);
    }

    return true;
}

template <typename ColumnT>
bool stringAsStringListArgument(const Column* input,
                                size_t row,
                                std::string_view,
                                std::vector<types::String::Primitive>& strings) {
    strings.clear();

    const std::optional<types::String::Primitive> string = (*static_cast<const ColumnT*>(input))[row];
    if (!string.has_value()) {
        return false;
    }

    strings.push_back(*string);

    return true;
}

template <typename ColumnT>
bool listAsStringListArgument(const Column* input,
                              size_t row,
                              std::string_view functionName,
                              std::vector<types::String::Primitive>& strings) {
    strings.clear();

    const std::optional<ListView> list = (*static_cast<const ColumnT*>(input))[row];

    return list.has_value() && listStrings(*list, functionName, strings);
}

template <typename ColumnT>
bool cellAsStringListArgument(const Column* input,
                              size_t row,
                              std::string_view functionName,
                              std::vector<types::String::Primitive>& strings) {
    strings.clear();

    const CellView cell = (*static_cast<const ColumnT*>(input))[row];
    const CellTag tag = cell.getTag();

    if (tag == CellTag::String) {
        strings.push_back(cell.getAs<types::String::Primitive>());
        return true;
    } else if (tag == CellTag::ListView) {
        return listStrings(cell.getAs<ListView>(), functionName, strings);
    } else if (tag == CellTag::Null) {
        return false;
    }

    throwCellTypeError(functionName, "a string or a list of strings", tag);
}

std::optional<types::Int64::Primitive> integerValueArgument(const Column* input, size_t row, std::string_view) {
    return (*static_cast<const ColumnOptVector<types::Int64::Primitive>*>(input))[row];
}

std::optional<types::Int64::Primitive> unsignedValueArgument(const Column* input, size_t row, std::string_view functionName) {
    const std::optional<types::UInt64::Primitive>& cell =
        (*static_cast<const ColumnOptVector<types::UInt64::Primitive>*>(input))[row];
    if (!cell.has_value()) {
        return std::nullopt;
    }

    return unsignedCharacterArgument(*cell, functionName);
}

template <typename ColumnT>
std::optional<types::Point::Primitive> pointValueArgument(const Column* input, size_t row, std::string_view) {
    return (*static_cast<const ColumnT*>(input))[row];
}

template <typename ColumnT>
std::optional<types::Point::Primitive> cellPointArgument(const Column* input, size_t row, std::string_view functionName) {
    const CellView cell = (*static_cast<const ColumnT*>(input))[row];
    const CellTag tag = cell.getTag();

    if (tag == CellTag::Point) {
        return cell.getAs<types::Point::Primitive>();
    } else if (tag == CellTag::Null) {
        return std::nullopt;
    }

    throwCellTypeError(functionName, "a point", tag);
}

std::optional<types::Int64::Primitive> cellIntegerArgument(const Column* input, size_t row, std::string_view functionName) {
    return cellInteger((*static_cast<const ColumnVector<CellView>*>(input))[row], functionName);
}

// A key spelled null in the query holds no value to name an entry by, on any row
std::optional<types::String::Primitive> nullStringRead(const Column*, size_t) {
    return std::nullopt;
}

std::optional<ListView> cellListRead(const CellView cell) {
    if (holdsNull(cell)) {
        return std::nullopt;
    } else if (holdsAList(cell)) {
        return operandList(cell);
    }

    throw IRException(fmt::format("A list slice or a percentile reads a list, not '{}'", queryTypeName(cell.getTag())));
}

std::optional<ListView> taggedListRead(const Column* input, size_t row) {
    return cellListRead((*static_cast<const ColumnVector<CellView>*>(input))[row]);
}

std::optional<ListView> constTaggedListRead(const Column* input, size_t) {
    return cellListRead(static_cast<const ColumnConst<CellView>*>(input)->getRaw());
}

struct SortField {
    std::string_view _key;
    bool _ascending {false};
};

void readSortFields(const ListView names, std::vector<SortField>& fields) {
    fields.clear();

    for (const CellView name : names) {
        if (name.getTag() != CellTag::String) {
            throw IRException("coll.sortMulti sorts on fields named by strings");
        }

        const std::string_view text = name.getAs<types::String::Primitive>();
        const bool ascending = text.starts_with('^');

        fields.push_back(SortField {._key = ascending ? text.substr(1) : text, ._ascending = ascending});
    }
}

std::optional<CellView> sortKey(const MapView map, const std::string_view key) {
    MapEntryView entry;
    if (!findMapEntry(map, key, entry)) {
        return std::nullopt;
    }

    const CellView value = entry.getValue();
    if (value.getTag() == CellTag::Null) {
        return std::nullopt;
    }

    return value;
}

// A missing key is a null, which APOC's sortMulti orders before every value
std::strong_ordering orderOnKeys(std::span<const std::optional<CellView>> lhs,
                                 std::span<const std::optional<CellView>> rhs,
                                 std::span<const SortField> fields) {
    for (size_t field = 0; field < fields.size(); field++) {
        const std::optional<CellView>& lhsKey = lhs[field];
        const std::optional<CellView>& rhsKey = rhs[field];

        std::strong_ordering order = std::strong_ordering::equal;
        if (lhsKey && rhsKey) {
            order = *lhsKey <=> *rhsKey;
        } else if (lhsKey) {
            order = std::strong_ordering::greater;
        } else if (rhsKey) {
            order = std::strong_ordering::less;
        }

        if (order != 0) {
            return fields[field]._ascending ? order : 0 <=> order;
        }
    }

    return std::strong_ordering::equal;
}

template <typename ColumnT>
NLContainer listContainer(const Column* input, size_t row) {
    const std::optional<ListView> list = (*static_cast<const ColumnT*>(input))[row];
    if (!list) {
        return NLContainer {};
    }

    return NLContainer {._list = list, ._keyed = NLKeylessValue {CellTag::ListView}};
}

template <typename ColumnT>
NLContainer mapContainer(const Column* input, size_t row) {
    return NLContainer {._keyed = mapRead<ColumnT>(input, row)};
}

template <typename ColumnT>
NLContainer cellContainer(const Column* input, size_t row) {
    const CellView cell = (*static_cast<const ColumnT*>(input))[row];
    if (!holdsAList(cell)) {
        return NLContainer {._keyed = readKeyedCell(cell)};
    }

    return NLContainer {._list = operandList(cell), ._keyed = NLKeylessValue {CellTag::ListView}};
}

NLSubscript subscriptOf(const CellView cell) {
    const CellTag tag = cell.getTag();

    switch (tag) {
        case CellTag::Int:
            return cell.getAs<types::Int64::Primitive>();
        break;
        case CellTag::UInt:
            return cell.getAs<types::UInt64::Primitive>();
        break;
        case CellTag::String:
            return cell.getAs<types::String::Primitive>();
        break;
        case CellTag::Null:
            return std::monostate {};
        break;
        case CellTag::Double:
        case CellTag::Bool:
        case CellTag::Embedding:
        case CellTag::ListView:
        case CellTag::NodeID:
        case CellTag::EdgeID:
        case CellTag::DateTime:
        case CellTag::MapView:
        case CellTag::Duration:
        case CellTag::Path:
        case CellTag::Point:
        case CellTag::LabelID:
        case CellTag::EdgeTypeID:
        case CellTag::PropertyTypeID:
        case CellTag::ValueType:
            return NLNeitherPositionNorKey {tag};
        break;
        case CellTag::INVALID:
            throw IRException("an index reads an untagged cell");
        break;
    }

    throw IRException("Unknown CellTag");
}

template <typename ColumnT>
NLSubscript readSubscript(const Column* input, size_t row) {
    return subscriptOf((*static_cast<const ColumnT*>(input))[row]);
}

// A constant stands for every row of the other operand, which a WHERE may have left empty
size_t pairedRowCount(const Column* lhs, const Column* rhs) {
    const bool lhsIsConstant = lhs->getContainerKind() == ContainerKind::code<ColumnConst>();
    const bool rhsIsConstant = rhs->getContainerKind() == ContainerKind::code<ColumnConst>();

    if (lhsIsConstant && rhsIsConstant) {
        return 1;
    } else if (lhsIsConstant) {
        return rhs->size();
    } else if (rhsIsConstant) {
        return lhs->size();
    }

    bioassert(lhs->size() == rhs->size(), "The two operands of a row-wise read are not row-aligned.");

    return lhs->size();
}

CellView readContainerElement(const NLContainer& container,
                              const NLSubscript& subscript,
                              NLEntityProperties& properties,
                              MapBuffer<>& mapBuffer) {
    const bool readsANull = std::holds_alternative<std::monostate>(subscript)
                         || std::holds_alternative<std::monostate>(container._keyed);
    const bool readsAPositionOfAList = container._list.has_value()
                                    && !std::holds_alternative<NLNeitherPositionNorKey>(subscript);

    if (readsANull) {
        return CellView::nullElement();
    } else if (const types::String::Primitive* key = std::get_if<types::String::Primitive>(&subscript)) {
        MapEntryView entry;
        if (!findKeyedEntry(container._keyed, *key, properties, mapBuffer, entry)) {
            return CellView::nullElement();
        }

        return entry.getValue();
    } else if (!readsAPositionOfAList) {
        throwIndexTypeError(heldTag(container._keyed), heldTag(subscript));
    } else if (const types::Int64::Primitive* position = std::get_if<types::Int64::Primitive>(&subscript)) {
        return ListIndexImpl {}(*container._list, *position);
    }

    return ListIndexImpl {}(*container._list, std::get<types::UInt64::Primitive>(subscript));
}

template <typename Element>
Element containerIndexResult(CellView element) {
    if constexpr (std::is_same_v<Element, CellView>) {
        return element;
    } else if constexpr (TypeUtils::is_optional_v<Element>) {
        return ValueListIndexImpl<typename Element::value_type>::elementValue(element);
    } else {
        return ValueListIndexImpl<Element>::elementValue(element).value_or(Element {});
    }
}

template <typename Primitive>
std::optional<types::Int64::Primitive> rangeBound(const Column* input, size_t row) {
    const std::optional<Primitive>& cell = (*static_cast<const ColumnOptVector<Primitive>*>(input))[row];
    if (!cell.has_value()) {
        return std::nullopt;
    }

    return static_cast<types::Int64::Primitive>(*cell);
}

std::optional<types::Int64::Primitive> cellBound(const CellView cell) {
    const CellTag tag = cell.getTag();

    if (tag == CellTag::Null) {
        return std::nullopt;
    } else if (cellHoldsAnInteger(cell)) {
        return cellInteger(cell);
    }

    throw IRException(fmt::format("A range or a list slice reads an integer bound, not '{}'", queryTypeName(tag)));
}

template <typename Result, Result (*ReadCell)(const CellView), typename ColumnT>
Result readCellRow(const Column* input, size_t row) {
    return ReadCell((*static_cast<const ColumnT*>(input))[row]);
}

template <typename Result, Result (*ReadCell)(const CellView), typename ColumnT>
Result readConstCell(const Column* input, size_t) {
    return ReadCell(static_cast<const ColumnT*>(input)->getRaw());
}

template <typename Result>
using CellRowRead = Result (*)(const Column* input, size_t row);

// The read @param ReadCell makes of a row of a column of type-erased cells, in whichever of
// the two layouts the column holds them; null for a column of another kind
template <typename Result, Result (*ReadCell)(const CellView)>
CellRowRead<Result> selectCellRowRead(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnVector<CellView>::staticKind()) {
        return &readCellRow<Result, ReadCell, ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &readConstCell<Result, ReadCell, ColumnConst<CellView>>;
    }

    return nullptr;
}

template <typename Primitive>
std::optional<types::Double::Primitive> percentileArgument(const Column* input, size_t row) {
    const std::optional<Primitive>& cell = (*static_cast<const ColumnOptVector<Primitive>*>(input))[row];
    if (!cell.has_value()) {
        return std::nullopt;
    }

    return static_cast<types::Double::Primitive>(*cell);
}

std::optional<types::Double::Primitive> cellPercentile(const CellView cell) {
    const CellTag tag = cell.getTag();
    if (tag == CellTag::Null) {
        return std::nullopt;
    }

    const std::optional<types::Double::Primitive> number = cellNumber(cell);
    if (!number) {
        throw IRException(fmt::format("percentileCont() and percentileDisc() take a percentile between 0.0 and 1.0, not '{}'",
                                      queryTypeName(tag)));
    }

    return number;
}

// The numbers one row's list holds, sorted. A percentile is taken of numbers alone, so any
// other element is the error the row raises.
void sortListNumbers(const ListView list, std::vector<CellView>& numbers, std::string_view functionName) {
    numbers.clear();

    for (const CellView element : list) {
        const CellTag tag = element.getTag();
        const bool isNumber = tag == CellTag::Int
                           || tag == CellTag::UInt
                           || tag == CellTag::Double;

        if (tag == CellTag::Null) {
            continue;
        } else if (!isNumber) {
            throw IRException(fmt::format("{}() takes the percentile of numbers, not '{}'", functionName, queryTypeName(tag)));
        }

        numbers.push_back(element);
    }

    std::ranges::sort(numbers, std::less<> {});
}

void throwIfNotAPercentile(double percentile, std::string_view functionName) {
    const bool isAFraction = percentile >= 0.0 && percentile <= 1.0;
    if (!isAFraction) {
        throw IRException(fmt::format("{}() takes a percentile between 0.0 and 1.0, not {}", functionName, percentile));
    }
}

// The value at the percentile's position among the sorted numbers, interpolated between
// the two around it where the position falls between them
double continuousPercentile(const std::vector<CellView>& sorted, double percentile) {
    const double position = percentile * static_cast<double>(sorted.size() - 1);
    const size_t below = static_cast<size_t>(position);
    const size_t above = static_cast<size_t>(std::ceil(position));

    const double lower = taggedNumericValue(sorted[below]);
    if (below == above) {
        return lower;
    }

    const double upper = taggedNumericValue(sorted[above]);

    return lower * (static_cast<double>(above) - position) + upper * (position - static_cast<double>(below));
}

// The first of the sorted numbers at or past the percentile's share of them
size_t discretePercentilePosition(size_t count, double percentile) {
    const size_t covering = static_cast<size_t>(std::ceil(percentile * static_cast<double>(count)));

    return covering == 0 ? 0 : covering - 1;
}

template <typename Result>
Result absentPercentile() {
    if constexpr (std::is_same_v<Result, CellView>) {
        return CellView::nullElement();
    } else {
        return std::nullopt;
    }
}

template <typename Result>
Result percentileElement(const CellView element) {
    if constexpr (std::is_same_v<Result, CellView>) {
        return element;
    } else {
        return element.getAs<typename Result::value_type>();
    }
}

// Row r of the result is what @param percentileOf answers for the sorted numbers of row
// r's list, or no value where the list holds none. The percentile is checked only beside
// numbers, as Neo4j reads it only off a row holding one.
template <typename Result, typename PercentileOf>
void runPercentileRows(const NLPercentileData* data, std::string_view functionName, PercentileOf percentileOf) {
    const Column* const list = data->getList();
    const NLListReadFunction listRead = data->getListRead();
    const Column* const percentile = data->getPercentile();
    const NLPercentileReadFunction percentileRead = data->getPercentileRead();

    const size_t rowCount = list->size();
    bioassert(percentile->size() == rowCount, "Percentile column of a {}() is not row-aligned with its lists.", functionName);

    std::vector<Result>& outputRaw = static_cast<ColumnVector<Result>*>(data->getResult())->getRaw();
    outputRaw.resize(rowCount);

    std::vector<CellView> numbers;

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<ListView> values = listRead(list, rowIndex);
        if (values) {
            sortListNumbers(*values, numbers, functionName);
        } else {
            numbers.clear();
        }

        if (numbers.empty()) {
            outputRaw[rowIndex] = absentPercentile<Result>();
            continue;
        }

        const std::optional<double> fraction = percentileRead(percentile, rowIndex);
        if (!fraction) {
            throw IRException(fmt::format("{}() takes a percentile between 0.0 and 1.0, not null", functionName));
        }

        throwIfNotAPercentile(*fraction, functionName);
        outputRaw[rowIndex] = percentileOf(numbers, *fraction);
    }
}

// The sibling of valueItem for a column whose cells are present in every row: a nested
// list or map, held as the one value it is.
template <typename Item, typename Element>
Item plainItem(const Column* input, size_t row, NLStepBuffers*) {
    return Item {(*static_cast<const ColumnVector<Element>*>(input))[row]};
}

template <typename Item, typename Element>
Item optItem(const Column* input, size_t row, NLStepBuffers*) {
    const std::optional<Element>& cell = (*static_cast<const ColumnOptVector<Element>*>(input))[row];
    if (!cell.has_value()) {
        return Item {PropertyNull {}};
    }

    return Item {*cell};
}

// The entity sibling of valueItem: an entity an OPTIONAL MATCH did not match is an invalid
// ID, which is how a null entity is spelled, so it is stored as the tagged null rather than
// as the value 2^64-1 - the null collectValidIDFold drops instead.
template <typename Item, typename IDType>
Item validIDItem(const Column* input, size_t row, NLStepBuffers*) {
    const IDType id = (*static_cast<const ColumnVector<IDType>*>(input))[row];
    if (!id.isValid()) {
        return Item {PropertyNull {}};
    }

    return Item {id};
}

// The path sibling of validIDItem: the path an OPTIONAL MATCH left where its pattern missed
// holds no node, and is stored as the tagged null. A kept path views the IDs its column holds
// for the step, as a string element views the characters of its column.
template <typename Item>
Item pathItem(const Column* input, size_t row, NLStepBuffers*) {
    const GraphPath& path = (*static_cast<const ColumnVector<GraphPath>*>(input))[row];
    if (path.isNull()) {
        return Item {PropertyNull {}};
    }

    return Item {PathView {path.getIDs()}};
}

// The sibling of valueItem for a type-erased column: the cell already carries the tag its
// value is stored under, so it is stored as the type that tag names.
template <typename Item>
Item taggedColumnItem(const Column* input, size_t row, NLStepBuffers*) {
    return taggedItem<Item>((*static_cast<const ColumnVector<CellView>*>(input))[row]);
}

// The fold and the list emit an entity collect of this ID reads. The list emits through
// the value path's template - its elements are the IDs the fold appended - so there is
// no entity emit of its own.
template <typename IDType>
void selectCollectIDHandlers(bool distinctValues,
                             NLCollectFoldFunction& fold,
                             NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectValidIDFoldDistinct<IDType> : &collectValidIDFold<IDType>;
    listEmit = &collectListEmit<IDType>;
}

template <typename Element>
void writeFinalizedRow(ColumnVector<Element>* output, size_t row, const std::type_identity_t<Element>& value) {
    output->getRaw()[row] = value;
}

template <typename Primitive>
void writeFinalizedRow(ColumnOptVector<Primitive>* output,
                       size_t row,
                       const std::type_identity_t<std::optional<Primitive>>& value) {
    output->setOptional(row, value);
}

// Finalize the states of groups [begin, begin + count) into the rows of an output column,
// one row per group
template <typename State, typename OutputColumn, typename Finalize>
void finalizeGroupStates(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, Finalize finalize) {
    OutputColumn* typedOutput = static_cast<OutputColumn*>(output);
    typedOutput->resize(count);

    for (size_t index = 0; index < count; index++) {
        writeFinalizedRow(typedOutput, index, finalize(places.getState<State>(begin + index)));
    }
}

// Emit what a state holds as it is: a count's tally or a sum's present total
template <typename State, typename OutputColumn>
void groupEmitCopy(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    finalizeGroupStates<State, OutputColumn>(places, begin, count, output, [](const State& state) {
        return state;
    });
}

template <typename Primitive>
using KeptValueColumn = std::conditional_t<std::is_same_v<Primitive, CellView>,
                                           ColumnVector<CellView>,
                                           ColumnOptVector<Primitive>>;

template <typename Primitive>
void groupEmitKeptValue(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    using State = NLKeptValue<Primitive>;
    using Element = TypeUtils::nullable_t<Primitive>;

    finalizeGroupStates<State, KeptValueColumn<Primitive>>(places, begin, count, output, [](const State& kept) {
        if (!kept.isKept()) {
            return TypeUtils::nullValue<Element>();
        }

        return Element(kept.getValue());
    });
}

// Emit an average: the total divided by the count, a double over numbers and a duration over
// durations. A group with no value averages to null, matching Cypher.
template <typename Total>
void groupEmitAverage(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    using Average = decltype(averageOf(Total {}, uint64_t {0}));
    using State = NLAverageState<Total>;

    finalizeGroupStates<State, ColumnOptVector<Average>>(places, begin, count, output, [](const State& average) {
        if (average._count == 0) {
            return std::optional<Average>();
        } else {
            return std::optional<Average>(averageOf(average._total, average._count));
        }
    });
}

void groupEmitDurationSum(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    using OutputColumn = ColumnOptVector<types::Duration::Primitive>;

    finalizeGroupStates<__int128, OutputColumn>(places, begin, count, output, [](__int128 total) {
        return std::optional<types::Duration::Primitive>(durationOfTotal(total));
    });
}

template <bool Population>
void groupEmitDeviation(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    finalizeGroupStates<NLDeviationState, ColumnOptVector<double>>(places, begin, count, output, [](const NLDeviationState& state) {
        return std::optional<double>(deviation<Population>(state._squaredDeviations, state._count));
    });
}

void groupEmitTaggedSum(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    QueryListBuffer& cells = stepBuffers->listBuffer();

    finalizeGroupStates<NLTaggedSum, ColumnVector<CellView>>(places, begin, count, output, [&](const NLTaggedSum& sum) {
        return sum.sum(cells);
    });
}

void groupEmitTaggedAverage(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    using State = NLAverageState<NLTaggedSum>;

    QueryListBuffer& cells = stepBuffers->listBuffer();

    finalizeGroupStates<State, ColumnVector<CellView>>(places, begin, count, output, [&](const State& average) {
        return average._total.average(average._count, cells);
    });
}

template <typename Primitive>
void describeGroupNumberSumState(NLGroupAggregateStateType& stateType) {
    describeGroupState<Primitive>(&groupEmitCopy<Primitive, ColumnOptVector<Primitive>>, stateType);
}

// The state of a grouped sum over a column of this value type: the running sum in the
// column's own type, or the 128-bit total of microseconds of durations
void describeGroupSumState(ValueType accumulatorType, NLGroupAggregateStateType& stateType) {
    switch (accumulatorType) {
        case ValueType::Int64:
            describeGroupNumberSumState<types::Int64::Primitive>(stateType);
        break;

        case ValueType::UInt64:
            describeGroupNumberSumState<types::UInt64::Primitive>(stateType);
        break;

        case ValueType::Double:
            describeGroupNumberSumState<types::Double::Primitive>(stateType);
        break;

        case ValueType::Duration:
            describeGroupState<__int128>(&groupEmitDurationSum, stateType);
        break;

        default:
            throw IRException("sum requires a numeric or a duration column");
        break;
    }
}

// The grouped sum fold for a column of this value type. sum adds the values, so
// only a numeric or a duration column is valid - the same restriction as the scalar
// selectSumUpdate.
NLGroupAggregateFoldFunction selectGroupSumFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldSum<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &groupFoldSum<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &groupFoldSum<types::Double::Primitive>;
        break;

        case ValueType::Duration:
            return &groupFoldDurationTotal</*Distinct=*/false, /*Averages=*/false>;
        break;

        default:
            throw IRException("sum requires a numeric or a duration column");
        break;
    }

    return nullptr;
}

// The grouped sum(DISTINCT x) fold for a column of this value type: sum's domain,
// reduced over each group's distinct values.
NLGroupAggregateFoldFunction selectGroupSumDistinctFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldSumDistinct<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &groupFoldSumDistinct<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &groupFoldSumDistinct<types::Double::Primitive>;
        break;

        case ValueType::Duration:
            return &groupFoldDurationTotal</*Distinct=*/true, /*Averages=*/false>;
        break;

        default:
            throw IRException("sum requires a numeric or a duration column");
        break;
    }

    return nullptr;
}

// The grouped avg fold for a column of this value type. avg widens each number to
// f64 and sums durations as durations, so - like sum - only a numeric or a duration
// column is valid.
NLGroupAggregateFoldFunction selectGroupAvgFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldAvg<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &groupFoldAvg<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &groupFoldAvg<types::Double::Primitive>;
        break;

        case ValueType::Duration:
            return &groupFoldDurationTotal</*Distinct=*/false, /*Averages=*/true>;
        break;

        case ValueType::Point:
            return &groupFoldAvg<types::Point::Primitive>;
        break;

        default:
            throw IRException("avg requires a numeric, a duration or a point column");
        break;
    }

    return nullptr;
}

// The grouped avg(DISTINCT x) fold for a column of this value type: avg's domain,
// averaged over each group's distinct values.
NLGroupAggregateFoldFunction selectGroupAvgDistinctFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldAvgDistinct<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &groupFoldAvgDistinct<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &groupFoldAvgDistinct<types::Double::Primitive>;
        break;

        case ValueType::Duration:
            return &groupFoldDurationTotal</*Distinct=*/true, /*Averages=*/true>;
        break;

        case ValueType::Point:
            return &groupFoldAvgDistinct<types::Point::Primitive>;
        break;

        default:
            throw IRException("avg requires a numeric, a duration or a point column");
        break;
    }

    return nullptr;
}

// The grouped min (IsMax false) / max (IsMax true) fold for a column of this value
// type. min/max order the values, so any orderable type is valid - numbers, bools,
// strings, lists and maps - but an embedding has no order, so it is rejected.
template <bool IsMax>
NLGroupAggregateFoldFunction selectGroupMinMaxFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldMinMax<types::Int64::Primitive, IsMax>;
        break;

        case ValueType::UInt64:
            return &groupFoldMinMax<types::UInt64::Primitive, IsMax>;
        break;

        case ValueType::Double:
            return &groupFoldMinMax<types::Double::Primitive, IsMax>;
        break;

        case ValueType::Bool:
            return &groupFoldMinMax<types::Bool::Primitive, IsMax>;
        break;

        case ValueType::String:
            return &groupFoldMinMax<types::String::Primitive, IsMax>;
        break;

        case ValueType::DateTime:
            return &groupFoldMinMax<types::DateTime::Primitive, IsMax>;
        break;

        case ValueType::Duration:
            return &groupFoldMinMax<types::Duration::Primitive, IsMax>;
        break;

        case ValueType::Point:
            return &groupFoldMinMax<types::Point::Primitive, IsMax>;
        break;

        case ValueType::List:
            return &groupFoldMinMax<types::List::Primitive, IsMax>;
        break;

        case ValueType::Map:
            return &groupFoldMinMax<types::Map::Primitive, IsMax>;
        break;

        default:
            throw IRException("min/max requires an orderable column");
        break;
    }

    return nullptr;
}

// The grouped first fold for a column of this value type. The kept value outlives its
// step as an extremum does, so first holds the types min/max hold.
NLGroupAggregateFoldFunction selectGroupFirstFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldFirst<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &groupFoldFirst<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &groupFoldFirst<types::Double::Primitive>;
        break;

        case ValueType::Bool:
            return &groupFoldFirst<types::Bool::Primitive>;
        break;

        case ValueType::String:
            return &groupFoldFirst<types::String::Primitive>;
        break;

        case ValueType::DateTime:
            return &groupFoldFirst<types::DateTime::Primitive>;
        break;

        case ValueType::Duration:
            return &groupFoldFirst<types::Duration::Primitive>;
        break;

        case ValueType::Point:
            return &groupFoldFirst<types::Point::Primitive>;
        break;

        case ValueType::List:
            return &groupFoldFirst<types::List::Primitive>;
        break;

        case ValueType::Map:
            return &groupFoldFirst<types::Map::Primitive>;
        break;

        default:
            throw IRException("first requires a column of scalars, lists or maps");
        break;
    }

    return nullptr;
}

// Execute a scan loop: the rows the graph holds, then the ones this change has written,
// which the pending step reads once the committed one is drained. @param rows is the chunk
// a step fills that measures it - the node chunk of a node scan, the source chunk of an
// edge scan.
template <typename ChunkWriterType, typename PendingScanType>
void runScanLoopSteps(NLExecutionContext* context,
                      ChunkWriterType* chunkWriter,
                      PendingScanType* pendingScan,
                      const ColumnNodeIDs* rows,
                      const NLStmtContainer* loopBody,
                      const NLLimitState* limit) {
    const auto hasStep = [&]() {
        return chunkWriter->isValid() || pendingScan->isValid();
    };

    const auto runIteration = [&]() {
        if (chunkWriter->isValid()) {
            chunkWriter->fill(context->getChunkSize());
        } else {
            pendingScan->fill(context->getChunkSize());
        }

        if (rows->empty()) {
            return;
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (hasStep() && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (hasStep()) {
            runIteration();
        }
    }
}

// Execute a get_out_edges/get_in_edges loop
template <typename ChunkWriterType>
void runEdgeLoopSteps(NLExecutionContext* context,
                      NLExpansionLoopData* loopData,
                      ChunkWriterType* chunkWriter,
                      NLPendingEdgeHop* pendingEdges,
                      ColumnNodeIDs* gatheredNodeIDs) {
    const NLStmtContainer* loopBody = loopData->getStmts();

    // Same early-exit as the scan loop: a null limit is unbounded, otherwise the
    // loop stops once the budget is spent, and the break unwinds any enclosing
    // loop carrying the same handle. The limit is fixed for the whole loop, so
    // the null check is hoisted out of the per-iteration condition.
    const NLLimitState* limit = loopData->getLimit();
    const size_t chunkSize = context->getChunkSize();

    const auto hasStep = [&]() {
        return chunkWriter->isValid() || (pendingEdges && pendingEdges->isValid());
    };

    const auto runIteration = [&](size_t rowBudget) {
        if (chunkWriter->isValid()) {
            chunkWriter->fill(rowBudget);
        } else {
            pendingEdges->fill(rowBudget);
        }

        const ColumnVector<size_t>* indices = loopData->getIndices();
        if (indices->empty()) {
            return;
        }

        // Column many not have been allocated (=nullptr) if its var wasn't used
        if (gatheredNodeIDs) {
            gatherColumn<NodeID>(loopData->getInput(), indices, gatheredNodeIDs);
        }

        // Transform all the columns in the carried set according to indices
        for (const NLCarriedColumn& carriedColumn : loopData->carriedColumns()) {
            const auto gatherFunc = carriedColumn.getGatherFunc();
            gatherFunc(carriedColumn.getInput(), indices, carriedColumn.getOutput());
        }

        runBody(context, loopBody);
    };

    if (limit) {
        // A body that filters keeps the budget unspent, so asking for exactly what the
        // limit still needs would pay a chunk's fixed cost once per surviving row: the
        // request doubles until it is back at a whole chunk.
        size_t rowBudget = 0;

        while (hasStep() && limit->getRemaining() > 0) {
            rowBudget = std::min(chunkSize, std::max(rowBudget * 2, limit->getRemaining()));

            runIteration(rowBudget);
        }
    } else {
        while (hasStep()) {
            runIteration(chunkSize);
        }
    }
}

// A column a merge produced mixes the two ID spaces, so its mask answers row by row;
// one with no mask is pending in every row or in none.
bool isPendingRow(const ColumnVector<CustomBool>* pending, bool isPendingColumn, size_t row) {
    if (pending) {
        return (*pending)[row];
    }

    return isPendingColumn;
}

// Which rows of an ID column name an entity this change has written and not committed. A
// column a write bound says so through the mask or the flag codegen put on the op; a hop
// produces its own rows, and the ID is what tells the two spaces apart there - this
// change's own start where the graph's end.
class PendingRows {
public:
    PendingRows(const ColumnVector<CustomBool>* pending, bool isPendingColumn, size_t committedCount, size_t pendingCount)
        : _pending(pending),
        _isPendingColumn(isPendingColumn),
        _committedCount(committedCount),
        _pendingCount(pendingCount)
    {
    }

    bool has(size_t row, uint64_t id) const {
        if (id == EntityID::max().getValue()) {
            return false;
        }

        const bool marked = isPendingRow(_pending, _isPendingColumn, row);
        const bool namesWritten = id >= _committedCount && id - _committedCount < _pendingCount;

        return marked || namesWritten;
    }

private:
    const ColumnVector<CustomBool>* _pending {nullptr};
    bool _isPendingColumn {false};
    size_t _committedCount {0};
    size_t _pendingCount {0};
};

// The write buffer knows a node this change wrote by its offset, so a row naming one is
// resolved back to it; every other row names a node the graph holds.
CommitWriteBuffer::ExistingOrPendingNode resolveNode(const ColumnNodeIDs* column,
                                                     size_t row,
                                                     const PendingRows& pendingRows,
                                                     size_t firstPendingNodeID) {
    const NodeID nodeID = (*column)[row];
    if (pendingRows.has(row, nodeID.getValue())) {
        return CommitWriteBuffer::PendingNodeOffset(nodeID.getValue() - firstPendingNodeID);
    } else {
        return nodeID;
    }
}

// The write buffer builds one column per property of a pending entity, so a second entry
// for the same property would be a second value for the same cell: ON CREATE writes over
// the value the pattern wrote a moment earlier rather than beside it.
void setPendingProperty(CommitWriteBuffer::UntypedProperties& properties,
                        const CommitWriteBuffer::UntypedProperty& property) {
    for (CommitWriteBuffer::UntypedProperty& held : properties) {
        if (held.propertyID == property.propertyID) {
            held.value = property.value;
            return;
        }
    }

    properties.push_back(property);
}

template <TypedInternalID IDT>
bool setWritesRow(const ColumnVector<CustomBool>* rows, IDT entity, size_t row) {
    return writeTouchesRow(rows, row) && entity.isValid();
}

template <typename IDColumn>
void markWrittenRows(const IDColumn* entities, const ColumnVector<CustomBool>* rows, ColumnVector<CustomBool>& written) {
    const auto& raw = entities->getRaw();
    written.resize(raw.size());

    for (size_t row = 0; row < raw.size(); row++) {
        written[row] = setWritesRow(rows, raw[row], row);
    }
}

// Stages the values a SET writes in @param buffer and answers the property it writes them
// to: invalid where it writes nothing
template <typename SetData>
PropertyTypeID stageSetProperty(MetadataBuilder* metadataBuilder,
                                SetData* setData,
                                size_t rowCount,
                                CommitWriteBuffer::UntypedProperties& buffer) {
    NLRuntimeProperty* runtimeProperty = setData->getRuntimeProperty();
    const PropertyTypeID propertyTypeID = setData->getPropertyTypeID();
    const Column* values = setData->getValue();

    if (!runtimeProperty) {
        extractColumnProperties(values, rowCount, propertyTypeID, buffer);
        return propertyTypeID;
    } else if (setData->writesNull()) {
        const bool staged = runtimeProperty->stageNulls(metadataBuilder, rowCount, buffer);
        return staged ? runtimeProperty->getType()._id : PropertyTypeID {};
    }

    ColumnVector<CustomBool>* written = setData->getWrittenRows();
    markWrittenRows(setData->getInput(), setData->getRows(), *written);

    const bool staged = runtimeProperty->stage(metadataBuilder, values, rowCount, written, buffer);

    return staged ? runtimeProperty->getType()._id : PropertyTypeID {};
}

// False for a chunk of nulls held in cells reaching a property with no type, which writes
// it nothing
bool stageCreatedProperty(MetadataBuilder* metadataBuilder,
                          const NLCreatedProperty& property,
                          size_t rowCount,
                          CommitWriteBuffer::UntypedProperties& buffer) {
    if (property._runtimeProperty) {
        return property._runtimeProperty->stage(metadataBuilder, property._values, rowCount, nullptr, buffer);
    }

    extractColumnProperties(property._values, rowCount, property._propertyTypeID, buffer);

    return true;
}

bool isSameMap(const WrittenMap& lhs, const WrittenMap& rhs) {
    const bool holdsAnotherKind = lhs._tag != rhs._tag || lhs._map.has_value() != rhs._map.has_value();

    if (holdsAnotherKind) {
        return false;
    } else if (!lhs._map) {
        return true;
    }

    const std::span<const MapEntryView> lhsEntries = lhs._map->entries();
    const std::span<const MapEntryView> rhsEntries = rhs._map->entries();

    return lhsEntries.data() == rhsEntries.data() && lhsEntries.size() == rhsEntries.size();
}

template <typename IDs, typename Visit>
void visitWrittenMapRows(const std::vector<WrittenMap>& maps,
                         const ColumnVector<CustomBool>* rows,
                         const IDs& raw,
                         size_t rowCount,
                         Visit visit) {
    const WrittenMap* previousMap = nullptr;

    for (size_t row = 0; row < rowCount; row++) {
        if (!setWritesRow(rows, raw[row], row)) {
            continue;
        }

        const bool holdsAnotherMap = !previousMap || !isSameMap(*previousMap, maps[row]);
        visit(row, holdsAnotherMap);

        previousMap = &maps[row];
    }
}

template <typename IDColumn>
void runSetProperties(NLExecutionContext* context, NLFunctionData* data) {
    constexpr bool WRITES_NODES = std::is_same_v<IDColumn, ColumnNodeIDs>;

    NLSetPropertiesData<IDColumn>* setData = static_cast<NLSetPropertiesData<IDColumn>*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "A properties write requires an active write transaction");

    const IDColumn* entities = setData->getInput();
    const size_t rowCount = entities->size();

    std::vector<WrittenMap>& maps = setData->getMaps();
    if (setData->writesNull()) {
        maps.assign(rowCount, WrittenMap {std::nullopt, CellTag::Null});
    } else {
        extractWrittenMaps(setData->getValue(), rowCount, maps);
    }

    const GraphView* view = context->getView();
    MetadataBuilder* metadataBuilder = context->getMetadataBuilder();
    const ColumnVector<CustomBool>* rows = setData->getRows();
    const bool replaces = setData->getReplaces();
    const auto& raw = entities->getRaw();

    std::vector<MapKeyPropertyType>& keyTypes = setData->getKeyTypes();
    std::vector<MapKeyCreatedType> createdTypes;

    forgetUntypedMapKeys(keyTypes);
    visitWrittenMapRows(maps, rows, raw, rowCount, [&](size_t row, bool holdsAnotherMap) {
        if (holdsAnotherMap) {
            widenCreatedMapKeyTypes(maps[row], view, metadataBuilder, keyTypes, createdTypes);
        }
    });

    for (const MapKeyCreatedType& created : createdTypes) {
        metadataBuilder->getOrCreatePropertyType(created._key, created._type);
    }

    CommitWriteBuffer::UntypedProperties& mapProperties = setData->getMapProperties();
    std::vector<StagedRange>& mapRanges = setData->getMapRanges();

    mapProperties.clear();
    mapRanges.assign(rowCount, StagedRange {});

    StagedRange stagedRange;

    forgetUntypedMapKeys(keyTypes);
    visitWrittenMapRows(maps, rows, raw, rowCount, [&](size_t row, bool holdsAnotherMap) {
        if (holdsAnotherMap) {
            const size_t begin = mapProperties.size();
            stageMapProperties(maps[row], view, metadataBuilder, keyTypes, mapProperties);

            stagedRange = StagedRange {begin, mapProperties.size()};
        }

        mapRanges[row] = stagedRange;
    });

    const size_t firstPendingID = WRITES_NODES ? committedNodeCount(view) : committedEdgeCount(view);
    const size_t pendingCount = WRITES_NODES ? writeBuffer->numPendingNodes() : writeBuffer->numPendingEdges();
    const PendingRows pendingRows(setData->getPending(), setData->isAllPending(), firstPendingID, pendingCount);
    NLWrittenValues& written = context->getWrittenValues();

    std::optional<NLEntityProperties> heldProperties;
    if (replaces) {
        heldProperties.emplace(context, nullptr);
    }

    CommitWriteBuffer::UntypedProperties& removals = setData->getRemovals();
    std::vector<PropertyTypeID>& held = setData->getHeld();
    std::vector<ValueType>& heldTypes = setData->getHeldTypes();

    for (size_t row = 0; row < rowCount; row++) {
        if (!setWritesRow(rows, raw[row], row)) {
            continue;
        }

        const StagedRange& range = mapRanges[row];
        const std::span<CommitWriteBuffer::UntypedProperty> writes {mapProperties.begin() + range._begin,
                                                                    mapProperties.begin() + range._end};

        removals.clear();
        if (replaces) {
            heldProperties->readIDs(raw[row], held);
            stageRemovals(held, writes, view, metadataBuilder, heldTypes, removals);
        }

        const uint64_t id = raw[row].getValue();

        if (pendingRows.has(row, id)) {
            const size_t offset = id - firstPendingID;

            if constexpr (WRITES_NODES) {
                CommitWriteBuffer::PendingNode& node = writeBuffer->getPendingNode(offset);
                const auto writeProperty = [&](const CommitWriteBuffer::UntypedProperty& property) {
                    setPendingProperty(node.properties, property);
                    written.addPendingNodeUpdate(offset, property.propertyID);
                };

                std::ranges::for_each(writes, writeProperty);
                std::ranges::for_each(removals, writeProperty);
            } else {
                CommitWriteBuffer::PendingEdge& edge = writeBuffer->getPendingEdge(offset);
                const auto writeProperty = [&](const CommitWriteBuffer::UntypedProperty& property) {
                    setPendingProperty(edge.properties, property);
                };

                std::ranges::for_each(writes, writeProperty);
                std::ranges::for_each(removals, writeProperty);
            }
        } else {
            const auto writeProperty = [&](CommitWriteBuffer::UntypedProperty& property) {
                if constexpr (WRITES_NODES) {
                    writeBuffer->addNodeUpdate(raw[row], property);
                } else {
                    writeBuffer->addEdgeUpdate(raw[row], property);
                }
            };

            std::ranges::for_each(writes, writeProperty);
            std::ranges::for_each(removals, writeProperty);
        }
    }
}

// The searched value as the write buffer holds one, so a scan can compare it against what
// a pending node was written with. A string the scan borrows is copied: the buffer's own
// values are owning.
template <typename T>
CommitWriteBuffer::SupportedTypeVariant pendingValueOf(typename T::Primitive value) {
    if constexpr (std::is_same_v<T, types::String>) {
        return std::optional<std::string> {std::string(value)};
    } else {
        return std::optional<typename T::Primitive> {value};
    }
}

template <typename T>
std::optional<typename T::Primitive> readPendingProperty(NLWrittenValues& written,
                                                         NLValueStore& values,
                                                         const CommitWriteBuffer::UntypedProperties& properties,
                                                         PropertyTypeID propertyTypeID) {
    for (const CommitWriteBuffer::UntypedProperty& property : properties) {
        if (property.propertyID == propertyTypeID) {
            return written.read<T>(property.value, values);
        }
    }

    return std::nullopt;
}

// A pending row names its entity by the ID it will commit as, so the write buffer is
// indexed by what is left once the committed space is taken off
template <typename ID, typename T>
std::optional<typename T::Primitive> readPendingEntityProperty(CommitWriteBuffer* writeBuffer,
                                                               NLWrittenValues& written,
                                                               NLValueStore& values,
                                                               const GraphView* view,
                                                               uint64_t id,
                                                               PropertyTypeID propertyTypeID) {
    if constexpr (std::is_same_v<ID, NodeID>) {
        const size_t offset = id - committedNodeCount(view);
        return readPendingProperty<T>(written, values, writeBuffer->getPendingNode(offset).properties, propertyTypeID);
    } else {
        const size_t offset = id - committedEdgeCount(view);
        return readPendingProperty<T>(written, values, writeBuffer->getPendingEdge(offset).properties, propertyTypeID);
    }
}

// A node an OPTIONAL MATCH did not match is an invalid ID, and no edge can hang off one.
// The whole column is read before anything is staged, so a CREATE naming a null endpoint
// writes none of its edges rather than leaving the commit one it cannot resolve. A pending
// endpoint is an offset into this change's new nodes rather than an ID, so only an existing
// one can be null.
void throwIfAnyNodeIsNull(const ColumnNodeIDs* column, bool isPending, std::string_view error) {
    if (isPending) {
        return;
    }

    for (const NodeID nodeID : column->getRaw()) {
        if (!nodeID.isValid()) {
            throw IRException(std::string(error));
        }
    }
}

void throwIfNodesHaveEdges(const GraphView& view, const ColumnNodeIDs* nodes) {
    const GetOutEdgesRange outEdges(view, nodes);
    for (const EdgeRecord& record : outEdges) {
        if (!view.isDeleted(record._edgeID)) {
            throw IRException("Cannot delete a node with relationships; use DETACH DELETE");
        }
    }

    const GetInEdgesRange inEdges(view, nodes);
    for (const EdgeRecord& record : inEdges) {
        if (!view.isDeleted(record._edgeID)) {
            throw IRException("Cannot delete a node with relationships; use DETACH DELETE");
        }
    }
}

// A merge keys the value a row asks for against the value the graph holds, and those
// arrive in a plain and a nullable column respectively: the two have to serialize alike,
// which is why a plain row keys its value as a nullable column keys a present one.
template <typename ElementType>
void mergeKeyAppendPlainColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();

    distinctAppendPresentValue(key, raw[row]);
}

template <typename ElementType>
void mergeKeyAppendConstColumn(const Column* column, size_t row, NLSerializedKey& key) {
    distinctAppendPresentValue(key, (*static_cast<const ColumnConst<ElementType>*>(column))[row]);
}

// The numeric siblings of the three above, keying the row's value in KeyType - the type
// the schema holds the property as - rather than in the one its column holds. The
// analyzer lets an integer constrain a double-typed property, and the graph side keys
// the double it reads back, so the two only compare equal once the row's value is
// converted to it.
template <typename ElementType, typename KeyType>
void mergeKeyAppendNumericPlainColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();

    distinctAppendPresentValue(key, static_cast<KeyType>(raw[row]));
}

template <typename ElementType, typename KeyType>
void mergeKeyAppendNumericConstColumn(const Column* column, size_t row, NLSerializedKey& key) {
    distinctAppendPresentValue(key, static_cast<KeyType>((*static_cast<const ColumnConst<ElementType>*>(column))[row]));
}

template <typename ElementType, typename KeyType>
void mergeKeyAppendNumericOptColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const ColumnOptVector<ElementType>* typedColumn = static_cast<const ColumnOptVector<ElementType>*>(column);

    if (!typedColumn->isValid(row)) {
        key.push_back('\0');
        return;
    }

    distinctAppendPresentValue(key, static_cast<KeyType>(typedColumn->getValue(row)));
}

// Keys the way a nullable column's null row does, so a merge on a null value binds the
// nodes that lack the property
void mergeKeyAppendConstNull(const Column* column, size_t row, NLSerializedKey& key) {
    key.push_back('\0');
}

// Which of the three shapes a merge property value column comes in, so one selector
// serves all of them
enum class MergeColumnShape {
    Plain,
    Const,
    Opt,
};

template <typename ElementType, typename KeyType, MergeColumnShape Shape>
NLKeyAppendFunction mergeKeyAppendFor() {
    if constexpr (Shape == MergeColumnShape::Plain) {
        return &mergeKeyAppendNumericPlainColumn<ElementType, KeyType>;
    } else if constexpr (Shape == MergeColumnShape::Const) {
        return &mergeKeyAppendNumericConstColumn<ElementType, KeyType>;
    } else {
        return &mergeKeyAppendNumericOptColumn<ElementType, KeyType>;
    }
}

// The appender for a numeric value column, keying its rows in the type the schema holds
// the property as. Identical types convert for free, so one path covers both.
template <typename ElementType, MergeColumnShape Shape>
NLKeyAppendFunction selectNumericMergeKeyAppend(ValueType keyType) {
    switch (keyType) {
        case ValueType::Int64:
            return mergeKeyAppendFor<ElementType, types::Int64::Primitive, Shape>();
        break;

        case ValueType::UInt64:
            return mergeKeyAppendFor<ElementType, types::UInt64::Primitive, Shape>();
        break;

        case ValueType::Double:
            return mergeKeyAppendFor<ElementType, types::Double::Primitive, Shape>();
        break;

        default:
            throw IRException("a MERGE pattern cannot constrain a non-numeric property to a number");
        break;
    }
}

// A non-numeric value converts to no other type the schema might hold the property as,
// so it can only key against a property of its own type
void throwUnlessKeyedAsItsOwnType(ValueType valueType, ValueType keyType) {
    if (valueType != keyType) {
        throw IRException("a MERGE pattern cannot constrain a property to a value of another type");
    }
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

template <SupportedType T>
void shortestPathSearch(NLExecutionContext* context, NLShortestPathLoopData* loopData) {
    using EdgePropType = typename T::Primitive;

    const GraphView& view = *context->getView();
    NLShortestPathStateImpl<T>* state = static_cast<NLShortestPathStateImpl<T>*>(loopData->getState());

    ColumnNodeIDs* input = loopData->getExpansionInput();
    ColumnEdgeIDs* outputEdges = loopData->getExpandedEdges();
    ColumnNodeIDs* outputNodes = loopData->getExpandedTargets();
    ColumnVector<size_t>* outputIndices = loopData->getExpandedIndices();
    ColumnOptVector<EdgePropType>* properties =
        static_cast<ColumnOptVector<EdgePropType>*>(loopData->getWeightValues());

    GetOutEdgesChunkWriter getOutEdgesWriter(view, input);
    getOutEdgesWriter.setIndices(outputIndices);
    getOutEdgesWriter.setEdgeIDs(outputEdges);
    getOutEdgesWriter.setTgtIDs(outputNodes);

    const PropertyTypeID weightType = state->getPropertyType();
    GetPropertiesWithNullChunkWriter<EdgeID, T> getPropertiesWriter(view, weightType, outputEdges);
    getPropertiesWriter.setOutput(properties);

    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    NLWrittenValues& written = context->getWrittenValues();
    if (writeBuffer) {
        written.indexUpdates(writeBuffer);
    }

    const bool hasUpdates = writeBuffer && written.hasUpdates();
    NLValueStore weightValues;

    const std::unordered_set<NodeID>& targetNodes = state->targets();

    typename NLShortestPathStateImpl<T>::DjistrakaHeap& heap = state->heap();
    typename NLShortestPathStateImpl<T>::DjistrakaValueMap& heapValueMap = state->heapValueMap();

    while (!heap.empty()) {
        const DjistrakaNode<EdgePropType> val = heap.top();

        if (targetNodes.contains(val.id)) {
            // target found
            break;
        }

        heap.pop();

        // We only need to confirm if the distance value is stale
        // as we only add a new value to the heap when we find a shorter
        // path to a node.
        const auto it = heapValueMap.find(val.id);
        if (it != heapValueMap.end() && it->second.distance != val.distance) {
            // remove stale value
            continue;
        }

        // Set up the input columns and iterators
        input->clear();
        input->push_back(val.id);
        getOutEdgesWriter.reset();
        // We set the chunk size to SIZE_MAX so that we expand the iterators
        // completely in 1 chunk
        getOutEdgesWriter.fill(std::numeric_limits<size_t>::max());

        getPropertiesWriter.reset();
        getPropertiesWriter.fill(std::numeric_limits<size_t>::max());

        if (hasUpdates) {
            for (size_t row = 0; row < properties->size(); row++) {
                const CommitWriteBuffer::SupportedTypeVariant* update = written.findEdgeUpdate((*outputEdges)[row], weightType);
                if (update) {
                    (*properties)[row] = written.read<T>(*update, weightValues);
                }
            }
        }

        // loop over all the edge properties
        for (size_t i = 0; i < properties->size(); ++i) {
            const std::optional<EdgePropType>& weight = (*properties)[i];

            if (!weight.has_value()) {
                continue;
            }

            if constexpr (std::is_signed_v<EdgePropType>) {
                if (*weight < 0) {
                    throw IRException("Cannot Do Shortest Path With Negative Weights");
                }
            }

            const auto outputNodeId = (*outputNodes)[i];
            const auto outputEdgeId = (*outputEdges)[i];
            const auto dist = val.distance + *weight;

            const auto it = heapValueMap.find(outputNodeId);
            if (it == heapValueMap.end()) {
                // If the node is not in the heapValueMap
                // then it is an unexplored node so we add it into heap
                // directly
                heap.push({outputNodeId, val.id, outputEdgeId, dist});
                heapValueMap[outputNodeId] = {val.id, outputEdgeId, dist};
            } else {
                // If we have come across this nodeID before we check if the new path
                // to the node is shorter than the current shortest path for the node
                // If so we replace it by pushing the new djistraka node into the heap
                // and changing the heapValueMap value to match the new shortest distance.
                //
                // The old path's node will still exist in the heap but if we ever pop
                // it from the heap we can compare it to the latest value in the heapValueMap
                // to invalidate it.
                if (dist < it->second.distance) {
                    heap.push({outputNodeId, val.id, outputEdgeId, dist});
                    heapValueMap[outputNodeId] = {val.id, outputEdgeId, dist};
                }
            }
        }
    }

    // Once we are out of the while loop if there are no values in the heap that means
    // no path was found between the source and target sets.

    if (heap.empty()) {
        return;
    }

    // After the loop the top value in the heap is the target node with the shortest
    // distance from the source set.

    Column* dist = loopData->getDistanceOutput();
    auto* distCol = static_cast<ColumnVector<EdgePropType>*>(dist);
    ColumnVector<GraphPath>* pathCol = loopData->getPathOutput();

    distCol->clear();
    pathCol->clear();

    // Get the total length of the path
    distCol->push_back(heap.top().distance);
    GraphPath& path = pathCol->emplace_back();

    // In the heap all the (non-stale) values represent the shortest path to that
    // nodeID from the source set. So we can find the shortet path by following the
    // previousNode back from the target node that we have found.
    auto lastNode = heap.top().prevNode;
    auto edge = heap.top().edge;
    path.start(heap.top().id);

    while (lastNode.isValid()) {
        path.addHop(edge, lastNode);

        const auto& pathInfo = heapValueMap[lastNode];
        lastNode = pathInfo.prevNode;
        edge = pathInfo.edge;
    }

    std::ranges::reverse(path.getIDs());

    runBody(context, loopData->getStmts());
}

}

NLExecutor::NLExecutor(const GraphView* view,
                       const NLProgram* prog,
                       NLOutputSink* sink,
                       CommitWriteBuffer* writeBuffer,
                       const NLSystemContext* system,
                       MetadataBuilder* metadataBuilder)
    : _ctxt(view, sink, prog->getChunkSize(), writeBuffer, system, metadataBuilder),
    _prog(prog)
{
}

NLExecutor::~NLExecutor() {
}

void NLExecutor::run() {
    NLOutputSink* const sink = _ctxt.getSink();
    if (sink) {
        NLOutputData* const outputData = _prog->getOutputData();
        std::span<const Column* const> outputs;
        if (outputData) {
            outputs = outputData->outputs();
        }

        sink->declareOutput(_prog->columnNames(), outputs);
    }

    runBody(&_ctxt, _prog->getStmts());
}

void NLExecutor::runCreateNode(NLExecutionContext* context, NLFunctionData* data) {
    NLCreateNodeData* createData = static_cast<NLCreateNodeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.create_node requires an active write transaction");

    const size_t rowCount = createData->getRowCount();
    const LabelSetHandle labelsetHandle = createData->getLabelSetHandle();

    MetadataBuilder* metadataBuilder = context->getMetadataBuilder();

    const std::vector<NLCreateNodeData::Property>& properties = createData->properties();
    std::vector<CommitWriteBuffer::UntypedProperties> extracted;
    extracted.reserve(properties.size());

    for (const NLCreateNodeData::Property& property : properties) {
        CommitWriteBuffer::UntypedProperties& values = extracted.emplace_back();

        if (!stageCreatedProperty(metadataBuilder, property, rowCount, values)) {
            extracted.pop_back();
        }
    }

    // Must extract this value before adding in the loop
    const size_t numPendingNodes = writeBuffer->numPendingNodes();

    for (size_t row = 0; row < rowCount; row++) {
        CommitWriteBuffer::PendingNode& node = writeBuffer->newPendingNode();
        node.labelsetHandle = labelsetHandle;

        for (const CommitWriteBuffer::UntypedProperties& values : extracted) {
            node.properties.push_back(values[row]);
        }
    }

    const GraphView* view = context->getView();
    const NodeID nextNodeID = committedNodeCount(view) + numPendingNodes;

    ColumnNodeIDs* result = createData->getResult();
    result->resize(rowCount);
    auto& raw = result->getRaw();
    for (size_t row = 0; row < rowCount; row++) {
        raw[row] = NodeID(nextNodeID + row);
    }
}

void NLExecutor::runMerge(NLExecutionContext* context, NLFunctionData* data) {
    NLMergeExecutor merge(context, static_cast<NLMergeData*>(data));
    merge.run();
}

void NLExecutor::runCreateEdge(NLExecutionContext* context, NLFunctionData* data) {
    NLCreateEdgeData* createData = static_cast<NLCreateEdgeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.create_edge requires an active write transaction");

    const ColumnNodeIDs* src = createData->getSrc();
    const ColumnNodeIDs* tgt = createData->getTgt();
    const bool srcIsPending = createData->isSrcPending();
    const bool tgtIsPending = createData->isTgtPending();
    const size_t rowCount = src->size();
    const EdgeTypeID edgeTypeID = createData->getEdgeTypeID();

    const ColumnVector<CustomBool>* srcPending = createData->getSrcPendingMask();
    const ColumnVector<CustomBool>* tgtPending = createData->getTgtPendingMask();

    throwIfAnyNodeIsNull(src, srcIsPending, "Cannot create an edge from a null node");
    throwIfAnyNodeIsNull(tgt, tgtIsPending, "Cannot create an edge to a null node");

    const GraphView* view = context->getView();
    const size_t firstPendingNodeID = committedNodeCount(view);
    const size_t numPendingNodes = writeBuffer->numPendingNodes();

    const PendingRows srcRows(srcPending, srcIsPending, firstPendingNodeID, numPendingNodes);
    const PendingRows tgtRows(tgtPending, tgtIsPending, firstPendingNodeID, numPendingNodes);

    MetadataBuilder* metadataBuilder = context->getMetadataBuilder();

    const std::vector<NLCreateEdgeData::Property>& properties = createData->properties();
    std::vector<CommitWriteBuffer::UntypedProperties> extracted;
    extracted.reserve(properties.size());

    for (const NLCreateEdgeData::Property& property : properties) {
        CommitWriteBuffer::UntypedProperties& values = extracted.emplace_back();

        if (!stageCreatedProperty(metadataBuilder, property, rowCount, values)) {
            extracted.pop_back();
        }
    }

    // Must extract this value before adding in the loop
    const size_t numPendingEdges = writeBuffer->numPendingEdges();

    for (size_t row = 0; row < rowCount; row++) {
        const CommitWriteBuffer::ExistingOrPendingNode srcNode = resolveNode(src, row, srcRows, firstPendingNodeID);
        const CommitWriteBuffer::ExistingOrPendingNode tgtNode = resolveNode(tgt, row, tgtRows, firstPendingNodeID);

        CommitWriteBuffer::PendingEdge& edge = writeBuffer->newPendingEdge(srcNode, tgtNode);
        edge.edgeType = edgeTypeID;

        for (const CommitWriteBuffer::UntypedProperties& values : extracted) {
            edge.properties.push_back(values[row]);
        }
    }

    const EdgeID nextEdgeID = committedEdgeCount(view) + numPendingEdges;

    ColumnEdgeIDs* result = createData->getResult();
    result->resize(rowCount);
    for (size_t row = 0; row < rowCount; row++) {
        (*result)[row] = EdgeID(nextEdgeID + row);
    }
}

void NLExecutor::runSetNodeProperty(NLExecutionContext* context, NLFunctionData* data) {
    NLSetNodePropertyData* setData = static_cast<NLSetNodePropertyData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.set_node_property requires an active write transaction");

    const ColumnNodeIDs* nodes = setData->getInput();
    const size_t rowCount = nodes->size();

    CommitWriteBuffer::UntypedProperties propsBuffer;
    const PropertyTypeID propID = stageSetProperty(context->getMetadataBuilder(), setData, rowCount, propsBuffer);
    if (!propID.isValid()) {
        return;
    }

    const ColumnVector<CustomBool>* pending = setData->getPending();
    const bool allPending = setData->isAllPending();
    const ColumnVector<CustomBool>* rows = setData->getRows();

    const size_t firstPendingNodeID = committedNodeCount(context->getView());
    const PendingRows pendingRows(pending, allPending, firstPendingNodeID, writeBuffer->numPendingNodes());
    NLWrittenValues& written = context->getWrittenValues();

    // A node an OPTIONAL MATCH did not match is an invalid ID, which Cypher writes nothing
    // for: staging it would have the commit look the ID up among the nodes this change
    // wrote, where it is not.
    const auto& raw = nodes->getRaw();
    for (size_t row = 0; row < rowCount; row++) {
        if (!setWritesRow(rows, raw[row], row)) {
            continue;
        }

        if (pendingRows.has(row, raw[row].getValue())) {
            const size_t offset = raw[row].getValue() - firstPendingNodeID;
            CommitWriteBuffer::PendingNode& node = writeBuffer->getPendingNode(offset);
            setPendingProperty(node.properties, propsBuffer[row]);
            written.addPendingNodeUpdate(offset, propID);
        } else {
            writeBuffer->addNodeUpdate(raw[row], propsBuffer[row]);
        }
    }
}

void NLExecutor::runSetEdgeProperty(NLExecutionContext* context, NLFunctionData* data) {
    NLSetEdgePropertyData* setData = static_cast<NLSetEdgePropertyData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.set_edge_property requires an active write transaction");

    const ColumnEdgeIDs* edges = setData->getInput();
    const size_t rowCount = edges->size();

    CommitWriteBuffer::UntypedProperties propsBuffer;
    const PropertyTypeID propID = stageSetProperty(context->getMetadataBuilder(), setData, rowCount, propsBuffer);
    if (!propID.isValid()) {
        return;
    }

    const ColumnVector<CustomBool>* pending = setData->getPending();
    const bool allPending = setData->isAllPending();
    const ColumnVector<CustomBool>* rows = setData->getRows();

    const size_t firstPendingEdgeID = committedEdgeCount(context->getView());
    const PendingRows pendingRows(pending, allPending, firstPendingEdgeID, writeBuffer->numPendingEdges());

    const auto& raw = edges->getRaw();
    for (size_t row = 0; row < rowCount; row++) {
        if (!setWritesRow(rows, raw[row], row)) {
            continue;
        }

        if (pendingRows.has(row, raw[row].getValue())) {
            CommitWriteBuffer::PendingEdge& edge =
                writeBuffer->getPendingEdge(raw[row].getValue() - firstPendingEdgeID);
            setPendingProperty(edge.properties, propsBuffer[row]);
        } else {
            writeBuffer->addEdgeUpdate(raw[row], propsBuffer[row]);
        }
    }
}

void NLExecutor::runSetNodeProperties(NLExecutionContext* context, NLFunctionData* data) {
    runSetProperties<ColumnNodeIDs>(context, data);
}

void NLExecutor::runSetEdgeProperties(NLExecutionContext* context, NLFunctionData* data) {
    runSetProperties<ColumnEdgeIDs>(context, data);
}

void NLExecutor::runDeleteNode(NLExecutionContext* context, NLFunctionData* data) {
    NLDeleteNodeData* deleteData = static_cast<NLDeleteNodeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.delete_node requires an active write transaction");

    const ColumnNodeIDs* nodes = deleteData->getInput();
    const GraphView* view = context->getView();
    const ColumnVector<CustomBool>* pending = deleteData->getPending();
    const bool allPending = deleteData->isAllPending();
    const bool detaching = deleteData->isDetaching();

    ColumnNodeIDs* committed = deleteData->getCommitted();
    committed->clear();

    const size_t firstPendingNodeID = committedNodeCount(view);
    const PendingRows pendingRows(pending, allPending, firstPendingNodeID, writeBuffer->numPendingNodes());

    const auto& raw = nodes->getRaw();

    // A node an OPTIONAL MATCH did not match is an invalid ID, which Cypher deletes nothing
    // for. Staging it would tombstone an ID the graph never held, and the node count reads
    // the tombstones off what it allocated: it would report one node too few.
    for (size_t row = 0; row < raw.size(); row++) {
        if (!raw[row].isValid()) {
            continue;
        }

        const CommitWriteBuffer::ExistingOrPendingNode node =
            resolveNode(nodes, row, pendingRows, firstPendingNodeID);

        // A relationship this query wrote counts as much as one the graph holds, whether
        // it hangs off a node the query wrote or off one already committed
        if (!detaching && writeBuffer->hasPendingEdgesOn(node)) {
            throw IRException("Cannot delete a node with relationships; use DETACH DELETE");
        }

        if (std::holds_alternative<CommitWriteBuffer::PendingNodeOffset>(node)) {
            writeBuffer->addDeletedPendingNode(std::get<CommitWriteBuffer::PendingNodeOffset>(node));
        } else {
            committed->push_back(raw[row]);
        }
    }

    if (!detaching) {
        throwIfNodesHaveEdges(*view, committed);
    }

    writeBuffer->addDeletedNodes(committed->getRaw());

    if (!detaching) {
        return;
    }

    writeBuffer->addHangingEdges(*view);
    writeBuffer->addHangingPendingEdges();
}

void NLExecutor::runDeleteEdge(NLExecutionContext* context, NLFunctionData* data) {
    NLDeleteEdgeData* deleteData = static_cast<NLDeleteEdgeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.delete_edge requires an active write transaction");

    const ColumnEdgeIDs* edges = deleteData->getInput();
    const ColumnVector<CustomBool>* pending = deleteData->getPending();
    const bool allPending = deleteData->isAllPending();

    ColumnEdgeIDs* committed = deleteData->getCommitted();
    committed->clear();

    const size_t firstPendingEdgeID = committedEdgeCount(context->getView());
    const PendingRows pendingRows(pending, allPending, firstPendingEdgeID, writeBuffer->numPendingEdges());

    const auto& raw = edges->getRaw();
    for (size_t row = 0; row < raw.size(); row++) {
        if (!raw[row].isValid()) {
            continue;
        }

        if (pendingRows.has(row, raw[row].getValue())) {
            writeBuffer->addDeletedPendingEdge(raw[row].getValue() - firstPendingEdgeID);
        } else {
            committed->push_back(raw[row]);
        }
    }

    writeBuffer->addDeletedEdges(committed->getRaw());
}

void NLExecutor::runScanNodesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanLoopData* loopData = static_cast<NLScanLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();

    // A null limit leaves the loop unbounded; otherwise it stops once the budget
    // is spent. nl.limit_update inside runBody mutates remaining, so the next
    // test breaks here, and an enclosing loop carrying the same handle breaks on
    // its next test too - unwinding the whole nest. LIMIT 0 fails the guard on
    // entry, so nothing is scanned. The limit is fixed for the whole loop, so
    // the null check is hoisted out of the per-iteration condition.
    const NLLimitState* limit = loopData->getLimit();

    ScanNodesChunkWriter chunkWriter(*context->getView());
    chunkWriter.setNodeIDs(nodeIDs);

    NLPendingNodeScan pendingNodes(context, nodeIDs);

    runScanLoopSteps(context, &chunkWriter, &pendingNodes, nodeIDs, loopBody, limit);
}

void NLExecutor::runScanNodesByLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanByLabelLoopData* loopData = static_cast<NLScanByLabelLoopData*>(data);

    // A requested label was absent from the schema, so no node carries the full
    // conjunction: the scan matches nothing and the loop body never runs.
    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();

    // A null limit leaves the loop unbounded, exactly as in runScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    // The LabelSetHandle borrows the loop data's owned LabelSet, which lives for
    // the whole program, so the handle stays valid for every fill below.
    const GraphView& view = *context->getView();
    const LabelSetHandle labelset(loopData->getLabelSet());

    ScanNodesByLabelChunkWriter chunkWriter(view, labelset);
    chunkWriter.setNodeIDs(nodeIDs);

    NLPendingNodeScan pendingNodes(context, nodeIDs);
    pendingNodes.setLabelSet(loopData->getLabelSet());

    runScanLoopSteps(context, &chunkWriter, &pendingNodes, nodeIDs, loopBody, limit);
}

template <SupportedType T>
void NLExecutor::runScanNodesByPropertyValueLoopAs(NLExecutionContext* context, NLScanByPropertyValueLoopData* loopData) {
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();
    const NLLimitState* limit = loopData->getLimit();

    // The literal and the label set are owned by the loop data, which lives for the whole
    // program, so a string view of the one and a handle on the other stay valid for
    // every fill below.
    const typename T::Primitive value = propertyScanLiteralAs<T>(loopData->getLiteral());
    const LabelSetHandle labelset = loopData->isByLabel() ? LabelSetHandle(loopData->getLabelSet()) : LabelSetHandle();

    NLPropertyValueScan<T> graphNodes(context, nodeIDs, loopData->getPropertyTypeID(), value, labelset);

    NLPendingNodeScan pendingNodes(context, nodeIDs);
    pendingNodes.setProperty(loopData->getPropertyTypeID(), pendingValueOf<T>(value));

    if (loopData->isByLabel()) {
        pendingNodes.setLabelSet(loopData->getLabelSet());
    }

    runScanLoopSteps(context, &graphNodes, &pendingNodes, nodeIDs, loopBody, limit);
}

void NLExecutor::runScanNodesByPropertyValueLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanByPropertyValueLoopData* loopData = static_cast<NLScanByPropertyValueLoopData*>(data);
    if (!loopData->isMatchable()) {
        return;
    }

    switch (loopData->getValueType()) {
        case ValueType::Int64:
            runScanNodesByPropertyValueLoopAs<types::Int64>(context, loopData);
        break;
        case ValueType::UInt64:
            runScanNodesByPropertyValueLoopAs<types::UInt64>(context, loopData);
        break;
        case ValueType::Double:
            runScanNodesByPropertyValueLoopAs<types::Double>(context, loopData);
        break;
        case ValueType::Bool:
            runScanNodesByPropertyValueLoopAs<types::Bool>(context, loopData);
        break;
        case ValueType::String:
            runScanNodesByPropertyValueLoopAs<types::String>(context, loopData);
        break;
        default:
            throw IRException("scan_nodes_by_property_value cannot scan this property type");
        break;
    }
}

void NLExecutor::runConstScanNodesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLConstScanLoopData* loopData = static_cast<NLConstScanLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();
    const std::span<const NodeID> constNodeIDs = loopData->getConstNodeIDs();
    const size_t chunkSize = context->getChunkSize();
    const size_t totalCount = constNodeIDs.size();
    const GraphView* view = context->getView();

    // A null limit leaves the loop unbounded, exactly as in runScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    // Emit the fixed node ID list one chunk at a time: each step copies the next
    // slice into the loop's node chunk and runs the body over it. The cursor is
    // local to this call, so a const scan nested in a cross product restarts from
    // the first ID on every outer step, the same way runScanNodesLoop opens a
    // fresh chunk writer each call.
    size_t cursor = 0;

    const auto runIteration = [&]() {
        const size_t remaining = totalCount - cursor;
        const size_t rows = std::min(chunkSize, remaining);

        std::vector<NodeID>& raw = nodeIDs->getRaw();
        raw.assign(constNodeIDs.begin() + cursor, constNodeIDs.begin() + cursor + rows);

        cursor += rows;

        // The translator dropped the IDs the graph had deleted, before any DELETE of this
        // query ran
        if (view->hasDeletedNodes()) {
            std::erase_if(raw, [view](NodeID node) { return view->isDeleted(node); });
        }

        if (raw.empty()) {
            return;
        }

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
    const std::string_view indexName = loopData->getIndexName();

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
    const std::span<const float> queryVector = loopData->getQueryVector();
    if (queryVector.size() != dimension) {
        throw IRException(fmt::format("Vector index '{}' holds vectors of dimension {}, but the "
                                      "query searches for one of dimension {}",
                                      indexName,
                                      dimension,
                                      queryVector.size()));
    }

    vec::VectorSearchQuery query(dimension);
    query.setVector(queryVector);
    query.setMaxResultCount(loopData->getNeighbourCount());

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

    std::vector<std::optional<types::Double::Primitive>>& neighbourScores = loopData->neighbourScores();
    neighbourScores.resize(distances.size());
    std::copy(distances.begin(), distances.end(), neighbourScores.begin());

    loopData->markSearched();
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
    if (!loopData->hasSearched()) {
        searchVectorIndex(context, loopData);
    }

    const std::span<const NodeID> ids = loopData->neighbourIDs();
    const std::span<const std::optional<types::Double::Primitive>> scores = loopData->neighbourScores();

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

        std::vector<std::optional<types::Double::Primitive>>& rawScores = scoreColumn->getRaw();
        rawScores.resize(rows);
        std::copy(scores.begin() + cursor, scores.begin() + cursor + rows, rawScores.begin());

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
    std::vector<std::optional<types::Int64::Primitive>>* const values = elements ? &elements->getRaw() : nullptr;
    ColumnVector<size_t>* rows = loopData->getRows();

    NLRangeCursor cursor(loopData);

    const auto runIteration = [&]() {
        cursor.fillChunk(chunkSize, values, rows->getRaw());

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

void NLExecutor::runScanEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesLoopData* loopData = static_cast<NLScanEdgesLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();

    // A null limit leaves the loop unbounded; otherwise it stops once the budget
    // is spent, exactly as in runScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    ScanEdgesChunkWriter chunkWriter(*context->getView());
    chunkWriter.setSrcIDs(sources);
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeScan pendingEdges(context, loopData);

    // The four columns are row-aligned, so the source column measures the step.
    runScanLoopSteps(context, &chunkWriter, &pendingEdges, sources, loopBody, limit);
}

void NLExecutor::runScanEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesByTypeLoopData* loopData = static_cast<NLScanEdgesByTypeLoopData*>(data);

    // No type the pattern named is in the schema, so no edge can match and the loop body never runs.
    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();

    const NLLimitState* limit = loopData->getLimit();

    ScanEdgesByTypeChunkWriter chunkWriter(*context->getView(), loopData->getRequestedTypes());
    chunkWriter.setSrcIDs(sources);
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeScan pendingEdges(context, loopData);
    pendingEdges.setEdgeTypes(loopData->getRequestedTypes());

    runScanLoopSteps(context, &chunkWriter, &pendingEdges, sources, loopBody, limit);
}

void NLExecutor::runScanEdgesBySourceLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesByLabelLoopData* loopData = static_cast<NLScanEdgesByLabelLoopData*>(data);

    // A requested label was absent from the schema, so no node carries the full
    // conjunction: the scan matches no edge and the loop body never runs.
    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();

    const NLLimitState* limit = loopData->getLimit();

    // The LabelSetHandle borrows the loop data's owned LabelSet, which lives for the
    // whole program, so the handle stays valid for every fill below.
    const LabelSetHandle labelset(loopData->getLabelSet());

    ScanOutEdgesBySourceLabelChunkWriter chunkWriter(*context->getView(), labelset);
    chunkWriter.setSrcIDs(sources);
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeScan pendingEdges(context, loopData);
    pendingEdges.setSourceLabelSet(loopData->getLabelSet());

    runScanLoopSteps(context, &chunkWriter, &pendingEdges, sources, loopBody, limit);
}

void NLExecutor::runScanEdgesByTargetLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesByLabelLoopData* loopData = static_cast<NLScanEdgesByLabelLoopData*>(data);

    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();

    const NLLimitState* limit = loopData->getLimit();

    const LabelSetHandle labelset(loopData->getLabelSet());

    // The writer names its two node columns after the walk rather than after the edge:
    // the labelled end it hangs each edge off goes to setSrcIDs, and here that end is the
    // edge's target. The chunks hold the edge as the graph stores it, the way
    // nl.get_in_edges fills them, so the two are crossed here.
    ScanInEdgesByTargetLabelChunkWriter chunkWriter(*context->getView(), labelset);
    chunkWriter.setSrcIDs(loopData->getTargets());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(sources);

    NLPendingEdgeScan pendingEdges(context, loopData);
    pendingEdges.setTargetLabelSet(loopData->getLabelSet());

    runScanLoopSteps(context, &chunkWriter, &pendingEdges, sources, loopBody, limit);
}

void NLExecutor::runScanEdgesByEndpointLabelsFromSourceLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesByEndpointLabelsLoopData* loopData = static_cast<NLScanEdgesByEndpointLabelsLoopData*>(data);

    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();

    const NLLimitState* limit = loopData->getLimit();

    const LabelSet& sourceLabelSet = loopData->getLabelSet();
    const LabelSet& targetLabelSet = loopData->getOtherEndLabelSet();
    const LabelSetHandle sourceLabels(sourceLabelSet);
    const LabelSetHandle targetLabels(targetLabelSet);

    ScanOutEdgesByBothLabelsChunkWriter chunkWriter(*context->getView(), sourceLabels, targetLabels);
    chunkWriter.setSrcIDs(sources);
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeScan pendingEdges(context, loopData);
    pendingEdges.setSourceLabelSet(sourceLabelSet);
    pendingEdges.setTargetLabelSet(targetLabelSet);

    runScanLoopSteps(context, &chunkWriter, &pendingEdges, sources, loopBody, limit);
}

void NLExecutor::runScanEdgesByEndpointLabelsFromTargetLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesByEndpointLabelsLoopData* loopData = static_cast<NLScanEdgesByEndpointLabelsLoopData*>(data);

    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();

    const NLLimitState* limit = loopData->getLimit();

    const LabelSet& targetLabelSet = loopData->getLabelSet();
    const LabelSet& sourceLabelSet = loopData->getOtherEndLabelSet();
    const LabelSetHandle targetLabels(targetLabelSet);
    const LabelSetHandle sourceLabels(sourceLabelSet);

    // The writer fills the indexed end through setSrcIDs, which here is the edge's target,
    // so the two node columns cross as in runScanEdgesByTargetLabelLoop.
    ScanInEdgesByBothLabelsChunkWriter chunkWriter(*context->getView(), targetLabels, sourceLabels);
    chunkWriter.setSrcIDs(loopData->getTargets());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(sources);

    NLPendingEdgeScan pendingEdges(context, loopData);
    pendingEdges.setTargetLabelSet(targetLabelSet);
    pendingEdges.setSourceLabelSet(sourceLabelSet);

    runScanLoopSteps(context, &chunkWriter, &pendingEdges, sources, loopBody, limit);
}

void NLExecutor::runGetOutEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetOutEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::Out, loopData->getTargets());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getSources());
}

void NLExecutor::runGetInEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetInEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setSrcIDs(loopData->getSources());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::In, loopData->getSources());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getTargets());
}

void NLExecutor::runGetEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setOtherIDs(loopData->getTargets());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::Either, loopData->getTargets());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getSources());
}

void NLExecutor::runGetOutEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByTypeLoopData* loopData = static_cast<NLEdgeByTypeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    // No type the pattern named is in the schema, so no edge can match; and an empty
    // input has no edges to walk. Either way the loop body never runs.
    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    GetOutEdgesByTypeChunkWriter chunkWriter(*context->getView(), inputNodeIDs, loopData->getRequestedTypes());
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::Out, loopData->getTargets());
    pendingEdges.setEdgeTypes(loopData->getRequestedTypes());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getSources());
}

void NLExecutor::runGetInEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByTypeLoopData* loopData = static_cast<NLEdgeByTypeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    GetInEdgesByTypeChunkWriter chunkWriter(*context->getView(), inputNodeIDs, loopData->getRequestedTypes());
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setSrcIDs(loopData->getSources());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::In, loopData->getSources());
    pendingEdges.setEdgeTypes(loopData->getRequestedTypes());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getTargets());
}

void NLExecutor::runGetOutEdgesByLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByLabelLoopData* loopData = static_cast<NLEdgeByLabelLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    // A requested label absent from the schema is carried by no node, and an empty input
    // has no edges to walk: either way the loop body never runs.
    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    // The LabelSetHandle borrows the loop data's owned LabelSet, which lives for the whole
    // program, so the handle stays valid for every fill below.
    const LabelSetHandle labelset(loopData->getLabelSet());

    GetOutEdgesByLabelChunkWriter chunkWriter(*context->getView(), inputNodeIDs, labelset);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::Out, loopData->getTargets());
    pendingEdges.setEndpointLabelSet(loopData->getLabelSet());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getSources());
}

void NLExecutor::runGetInEdgesByLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByLabelLoopData* loopData = static_cast<NLEdgeByLabelLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    const LabelSetHandle labelset(loopData->getLabelSet());

    GetInEdgesByLabelChunkWriter chunkWriter(*context->getView(), inputNodeIDs, labelset);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setSrcIDs(loopData->getSources());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::In, loopData->getSources());
    pendingEdges.setEndpointLabelSet(loopData->getLabelSet());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getTargets());
}

void NLExecutor::runGetOutEdgesByTypeAndLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByTypeAndLabelLoopData* loopData = static_cast<NLEdgeByTypeAndLabelLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    const std::span<const EdgeTypeID> requestedTypes = loopData->getRequestedTypes();
    const LabelSetHandle labelset(loopData->getLabelSet());

    GetOutEdgesByTypeAndLabelChunkWriter chunkWriter(*context->getView(), inputNodeIDs, requestedTypes, labelset);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::Out, loopData->getTargets());
    pendingEdges.setEdgeTypes(requestedTypes);
    pendingEdges.setEndpointLabelSet(loopData->getLabelSet());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getSources());
}

void NLExecutor::runGetInEdgesByTypeAndLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByTypeAndLabelLoopData* loopData = static_cast<NLEdgeByTypeAndLabelLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    const std::span<const EdgeTypeID> requestedTypes = loopData->getRequestedTypes();
    const LabelSetHandle labelset(loopData->getLabelSet());

    GetInEdgesByTypeAndLabelChunkWriter chunkWriter(*context->getView(), inputNodeIDs, requestedTypes, labelset);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setSrcIDs(loopData->getSources());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::In, loopData->getSources());
    pendingEdges.setEdgeTypes(requestedTypes);
    pendingEdges.setEndpointLabelSet(loopData->getLabelSet());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getTargets());
}

void NLExecutor::runEachRowLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEachRowLoopData* loopData = static_cast<NLEachRowLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    const NLEachRowLoopData::Columns& columns = loopData->columns();
    ColumnVector<size_t>* indices = loopData->getIndices();
    std::vector<size_t>& indicesRaw = indices->getRaw();

    // A null limit leaves the loop unbounded, exactly as in runUnwindConstLoop.
    const NLLimitState* limit = loopData->getLimit();
    const auto budgetLeft = [&]() { return !limit || limit->getRemaining() > 0; };

    const size_t rowCount = columns.front().getInput()->size();
    indicesRaw.resize(1);

    for (size_t row = 0; row < rowCount && budgetLeft(); row++) {
        indicesRaw[0] = row;

        for (const NLCarriedColumn& column : columns) {
            column.getGatherFunc()(column.getInput(), indices, column.getOutput());
        }

        runBody(context, loopBody);
    }
}

namespace {

// Runs the hop predicate of one step of an nl.explore_paths over a batch of frames: the
// candidates and the frame's repetition so far are copied a chunk at a time into the columns
// the hop statements read, and the survivors are compacted to the front of the batch. A chunk
// may start and end mid-frame. The hop_labels filter, when there is one, cuts the batch first.
class NLHopFilter : public PathHopFilter {
public:
    NLHopFilter(NLExecutionContext* context, const NLExploreStep* step, PathHopFilter* labelFilter)
        : _context(context),
        _step(step),
        _labelFilter(labelFilter)
    {
        _indices.reserve(context->getChunkSize());
    }

    ~NLHopFilter() override {
    }

    size_t filter(std::span<PathHopFrame> frames, std::span<NodeID> candidateNodes, std::span<EdgeID> candidateEdges) override {
        if (_labelFilter) {
            const size_t labelled = _labelFilter->filter(frames, candidateNodes, candidateEdges);
            candidateNodes = candidateNodes.first(labelled);
            candidateEdges = candidateEdges.first(labelled);
        }

        const size_t chunkSize = _context->getChunkSize();
        const std::span<ColumnNodeIDs* const> nodes = _step->getHopNodes();
        const std::span<ColumnEdgeIDs* const> edges = _step->getHopEdges();
        const size_t position = _step->getHopPosition();
        ColumnNodeIDs* sources = nodes[position];
        ColumnEdgeIDs* hopEdges = edges[position];
        ColumnNodeIDs* ends = nodes[position + 1];
        const Column* mask = _step->getHopMask();
        const NLMaskSurvivorFunction survivors = _step->getHopSurvivors();
        const NLStmtContainer* stmts = _step->getHopStmts();
        const std::span<const NLHopImport> imports = _step->getHopImports();

        _frameEnds.clear();
        size_t frameEnd = 0;
        for (PathHopFrame& frame : frames) {
            frameEnd += frame._candidateCount;
            _frameEnds.push_back(frameEnd);
            frame._candidateCount = 0;
        }

        std::vector<size_t>& seedRows = _seedRows.getRaw();
        size_t fillFrame = 0;
        size_t survivorFrame = 0;
        size_t kept = 0;
        for (size_t begin = 0; begin < candidateNodes.size(); begin += chunkSize) {
            const size_t count = std::min(chunkSize, candidateNodes.size() - begin);
            const size_t end = begin + count;

            sources->resize(count);
            seedRows.resize(count);
            for (size_t earlier = 0; earlier < position; earlier++) {
                if (nodes[earlier]) {
                    nodes[earlier]->resize(count);
                }

                if (edges[earlier]) {
                    edges[earlier]->resize(count);
                }
            }

            for (size_t segmentBegin = begin; segmentBegin < end;) {
                const size_t segmentEnd = std::min(end, _frameEnds[fillFrame]);
                const PathHopFrame& frame = frames[fillFrame];
                const size_t segmentOffset = segmentBegin - begin;
                const size_t segmentSize = segmentEnd - segmentBegin;
                std::fill_n(sources->begin() + segmentOffset, segmentSize, frame._source);
                std::fill_n(seedRows.begin() + segmentOffset, segmentSize, frame._seedRow);

                for (size_t earlier = 0; earlier < position; earlier++) {
                    if (nodes[earlier]) {
                        std::fill_n(nodes[earlier]->begin() + segmentOffset, segmentSize, frame._repetitionNodes[earlier]);
                    }

                    if (edges[earlier]) {
                        std::fill_n(edges[earlier]->begin() + segmentOffset, segmentSize, frame._repetitionEdges[earlier]);
                    }
                }

                if (segmentEnd == _frameEnds[fillFrame]) {
                    fillFrame++;
                }
                segmentBegin = segmentEnd;
            }

            hopEdges->resize(count);
            std::copy_n(candidateEdges.begin() + begin, count, hopEdges->begin());

            ends->resize(count);
            std::copy_n(candidateNodes.begin() + begin, count, ends->begin());

            for (const NLHopImport& import : imports) {
                import._gather(import._source, &_seedRows, import._chunk);
            }

            runBody(_context, stmts);

            _indices.getRaw().clear();
            survivors(mask, &_indices);

            for (const size_t survivor : _indices.getRaw()) {
                const size_t candidatePosition = begin + survivor;
                while (candidatePosition >= _frameEnds[survivorFrame]) {
                    survivorFrame++;
                }

                frames[survivorFrame]._candidateCount++;
                candidateNodes[kept] = candidateNodes[candidatePosition];
                candidateEdges[kept] = candidateEdges[candidatePosition];
                kept++;
            }
        }

        return kept;
    }

    bool readsRepetition() const override {
        const std::span<ColumnNodeIDs* const> nodes = _step->getHopNodes();
        const std::span<ColumnEdgeIDs* const> edges = _step->getHopEdges();
        const size_t position = _step->getHopPosition();

        const auto isRead = [](const Column* column) { return column != nullptr; };

        return std::any_of(nodes.begin(), nodes.begin() + position, isRead)
            || std::any_of(edges.begin(), edges.begin() + position, isRead);
    }

private:
    NLExecutionContext* _context {nullptr};
    const NLExploreStep* _step {nullptr};
    PathHopFilter* _labelFilter {nullptr};
    ColumnVector<size_t> _indices;
    ColumnVector<size_t> _seedRows;
    std::vector<size_t> _frameEnds;
};

// The direction and edge types every step of the body follows together: what a walk can reach
// over them bounds what the body reaches, so the pruning indexes are built over them
PathExplorationDir combinedDirection(const NLExplorePathsLoopData* loopData) {
    const NLExploreStep* firstStep = loopData->getStep(0);
    const PathExplorationDir first = firstStep->getDirection();
    const size_t stepCount = loopData->getStepCount();

    for (size_t step = 1; step < stepCount; step++) {
        const NLExploreStep* exploreStep = loopData->getStep(step);
        if (exploreStep->getDirection() != first) {
            return PathExplorationDir::BOTH;
        }
    }

    return first;
}

// Empty when a step follows every type
void combinedEdgeTypes(const NLExplorePathsLoopData* loopData, std::vector<EdgeTypeID>& edgeTypes) {
    edgeTypes.clear();

    const size_t stepCount = loopData->getStepCount();
    for (size_t step = 0; step < stepCount; step++) {
        const NLExploreStep* exploreStep = loopData->getStep(step);
        if (!exploreStep->filtersByType()) {
            edgeTypes.clear();
            return;
        }

        const std::span<const EdgeTypeID> stepTypes = exploreStep->getEdgeTypes();
        edgeTypes.insert(edgeTypes.end(), stepTypes.begin(), stepTypes.end());
    }

    std::sort(edgeTypes.begin(), edgeTypes.end());
    edgeTypes.erase(std::unique(edgeTypes.begin(), edgeTypes.end()), edgeTypes.end());
}

// The hop bounds of the loop count repetitions of the body, the walk's count its edges
uint64_t edgesOfRepetitions(uint64_t repetitions, size_t stepCount) {
    return llvm::SaturatingMultiply<uint64_t>(repetitions, stepCount);
}

// The body the walk repeats, as a sample expands it. A filter reading the repetition sees
// none in the sample, so its step is sampled unfiltered.
void sampledBodyOf(std::span<const PathStep> walkedBody, std::vector<PathStep>& body) {
    body.assign(walkedBody.begin(), walkedBody.end());
    for (PathStep& bodyStep : body) {
        const bool readsRepetition = bodyStep._hopFilter && bodyStep._hopFilter->readsRepetition();
        if (readsRepetition) {
            bodyStep._hopFilter = nullptr;
        }
    }
}

// The distance index of an end-constrained exploration is built at most once per loop, the
// first time the seeds seen over all its chunks make the enumeration costlier than the
// index; null until then
const PathDistanceIndex* pruningIndexFor(const GraphView& view,
                                         NLExplorePathsLoopData* loopData,
                                         uint64_t maxHops,
                                         const PathDistanceIndex::SeedExpansion& expansion) {
    PathDistanceIndex* index = loopData->getDistanceIndex();
    if (index->isBuilt()) {
        return index;
    }

    std::vector<EdgeTypeID> edgeTypes;
    combinedEdgeTypes(loopData, edgeTypes);

    const PathExplorationDir direction = combinedDirection(loopData);
    const PartDirectory parts(view);
    const double walkChecks = PathDistanceIndex::estimatedEnumerationChecks(parts, expansion, loopData->getSeedsSeen(), maxHops);
    const bool built = index->buildWithin(view, loopData->getEndLabels(), direction, edgeTypes, maxHops, walkChecks);

    return built ? index : nullptr;
}

// The target index of an end set is the whole walk's: one search from every node of the set,
// built once the seeds seen imply more enumeration than that search costs
const PathTargetIndex* endSetTargetIndexFor(const GraphView& view,
                                            NLExplorePathsLoopData* loopData,
                                            std::span<const NodeID> endNodes,
                                            uint64_t maxHops,
                                            const PathDistanceIndex::SeedExpansion& expansion) {
    PathTargetIndex* index = loopData->getTargetIndex();
    if (index->isBuilt()) {
        return index;
    }

    std::vector<EdgeTypeID> edgeTypes;
    combinedEdgeTypes(loopData, edgeTypes);

    const PathExplorationDir direction = combinedDirection(loopData);
    const bool worthBuilding = PathTargetIndex::isWorthBuildingSet(view, direction, edgeTypes, expansion, loopData->getSeedsSeen(), endNodes.size(), maxHops);
    if (!worthBuilding) {
        return nullptr;
    }

    index->buildSet(view, endNodes, direction, edgeTypes, maxHops);

    return index;
}

// The target index of a bound end is a chunk's own: its distinct targets, batched 64 per
// word, when the enumeration they imply is costlier than the batches; null otherwise
const PathTargetIndex* targetIndexFor(const GraphView& view,
                                      NLExplorePathsLoopData* loopData,
                                      uint64_t maxHops,
                                      const PathDistanceIndex::SeedExpansion& expansion) {
    const std::vector<NodeID>& endNodes = loopData->getEndNodes()->getRaw();

    std::vector<NodeID> targets(endNodes.begin(), endNodes.end());
    std::sort(targets.begin(), targets.end());
    targets.erase(std::unique(targets.begin(), targets.end()), targets.end());

    std::vector<EdgeTypeID> edgeTypes;
    combinedEdgeTypes(loopData, edgeTypes);

    const PathExplorationDir direction = combinedDirection(loopData);
    const bool worthBuilding = PathTargetIndex::isWorthBuilding(view, direction, edgeTypes, expansion, endNodes.size(), targets.size(), maxHops);
    if (!worthBuilding) {
        return nullptr;
    }

    PathTargetIndex* index = loopData->getTargetIndex();
    index->build(view, targets, direction, edgeTypes, maxHops);

    return index;
}

// A walk to an end set emits the same rows from either end when no path is read and no hop
// test sees which way the hop was taken
bool emitsTheSameFromEitherEnd(const NLExplorePathsLoopData* loopData, std::span<const PathStep> walkedBody) {
    const bool hasHopFilter = std::ranges::any_of(walkedBody, [](const PathStep& step) { return step._hopFilter != nullptr; });

    return !loopData->getPaths() && !hasHopFilter;
}

void runFromTheEndSet(NLExecutionContext* context,
                      NLExplorePathsLoopData* loopData,
                      ReversedPathExplorator::Body body,
                      std::span<const NodeID> endNodes,
                      uint64_t minHops,
                      uint64_t maxHops,
                      bool walksPendingEdges,
                      const PathDistanceIndex::SeedExpansion& fromEnds) {
    const GraphView& view = *context->getView();
    const LabelSet* endLabels = loopData->filtersByEndLabels() ? &loopData->getEndLabels() : nullptr;

    ReversedPathExplorator explorator(view,
                                      loopData->getInput(),
                                      endNodes,
                                      endLabels,
                                      body,
                                      minHops,
                                      maxHops);
    explorator.setIndices(loopData->getIndices());
    explorator.setTargets(loopData->getTargets());
    const bool distinctEnds = loopData->isDistinctEnds();
    explorator.setDistinctEnds(distinctEnds);

    if (context->getWriteBuffer()) {
        const PendingAdjacency& pendingAdjacency = context->getPendingAdjacency();
        explorator.setPendingAdjacency(&pendingAdjacency, pendingAdjacency.getEdgeIDBound());
    }

    std::vector<EdgeTypeID> edgeTypes;
    combinedEdgeTypes(loopData, edgeTypes);

    // The seeds are what this walk heads for, so they are the set its index is searched from
    PathTargetIndex seedIndex;
    const std::span<const NodeID> seedNodes = explorator.getSeedNodes();
    const PathExplorationDir reversed = reverseOf(combinedDirection(loopData));
    const bool indexesSeeds = !distinctEnds
                           && !walksPendingEdges
                           && PathTargetIndex::isWorthBuildingSet(view, reversed, edgeTypes, fromEnds, endNodes.size(), seedNodes.size(), maxHops);
    if (indexesSeeds) {
        seedIndex.buildSet(view, seedNodes, reversed, edgeTypes, maxHops);
        explorator.setTargetIndex(&seedIndex);
    }

    runEdgeLoopSteps(context, loopData, &explorator, nullptr, loopData->getSources());
}

}

void NLExecutor::runNodeSetReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLNodeSetResetData* reset = static_cast<NLNodeSetResetData*>(data);

    reset->getState()->reset();
}

void NLExecutor::runNodeSetCollect(NLExecutionContext* context, NLFunctionData* data) {
    const NLNodeSetCollectData* collect = static_cast<NLNodeSetCollectData*>(data);

    collect->getState()->add(collect->getNodes()->getRaw());
}

void NLExecutor::runExplorePathsLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLExplorePathsLoopData* loopData = static_cast<NLExplorePathsLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    // An end label absent from the schema is carried by no node, so no path can end
    const bool filtersByEndLabels = loopData->filtersByEndLabels();
    if (filtersByEndLabels && !loopData->isEndMatchable()) {
        return;
    }

    // The ends a whole set binds, gathered by the loop that closed before this one opened.
    // No node of the set is no node to end on, whatever the hop bounds say.
    NLNodeSetState* const endNodeSet = loopData->getEndNodeSet();
    std::span<const NodeID> endNodes;
    if (endNodeSet) {
        endNodes = endNodeSet->getNodes();
        if (endNodes.empty()) {
            return;
        }
    }

    // An edge type absent from the schema matches no edge, so nothing is ever expanded;
    // the zero-length rows of a min of zero still come out, so this is not an early return
    const size_t stepCount = loopData->getStepCount();
    const uint64_t minHops = edgesOfRepetitions(loopData->getMinHops(), stepCount);
    const uint64_t maxHops = loopData->isMatchable() ? edgesOfRepetitions(loopData->getMaxHops(), stepCount) : 0;

    const GraphView& view = *context->getView();
    PathExplorator explorator(view, inputNodeIDs, minHops, maxHops);
    explorator.setIndices(loopData->getIndices());
    explorator.setTargets(loopData->getTargets());

    if (loopData->getPaths()) {
        explorator.setPaths(loopData->getPaths(), loopData->getTrie());
    }

    const bool distinctEnds = loopData->isDistinctEnds();
    explorator.setDistinctEnds(distinctEnds);

    const CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    if (writeBuffer) {
        PendingAdjacency& pendingAdjacency = context->getPendingAdjacency();
        pendingAdjacency.index(*writeBuffer,
                               context->getFirstQueryEdge(),
                               committedNodeCount(&view),
                               committedEdgeCount(&view));
        explorator.setPendingAdjacency(&pendingAdjacency, pendingAdjacency.getEdgeIDBound());
    }

    // A change that wrote only nodes opens no path the committed graph does not already
    // hold, so only a pending edge puts the two pruning indexes below out of date.
    const bool walksPendingEdges = writeBuffer && writeBuffer->numPendingEdges() > context->getFirstQueryEdge();

    std::vector<std::optional<NLHopFilter>> hopRegionFilters(stepCount);
    std::vector<std::optional<PathLabelHopFilter>> hopLabelFilters(stepCount);
    std::vector<PathStep> walkedBody(stepCount);
    for (size_t step = 0; step < stepCount; step++) {
        const NLExploreStep* exploreStep = loopData->getStep(step);
        PathStep& bodyStep = walkedBody[step];
        bodyStep._direction = exploreStep->getDirection();

        if (exploreStep->filtersByType()) {
            bodyStep._edgeTypes = exploreStep->getEdgeTypes();
        }

        PathHopFilter* hopFilter = nullptr;
        if (exploreStep->filtersByHopLabels()) {
            const PendingAdjacency* pendingAdjacency = writeBuffer ? &context->getPendingAdjacency() : nullptr;
            hopFilter = &hopLabelFilters[step].emplace(view, exploreStep->getHopLabels(), exploreStep->areHopLabelsMatchable(), pendingAdjacency);
        }

        if (exploreStep->hasHopFilter()) {
            hopFilter = &hopRegionFilters[step].emplace(context, exploreStep, hopFilter);
        }

        bodyStep._hopFilter = hopFilter;
        explorator.addStep(bodyStep);
    }

    // The level search of the distinct mode prunes by no index
    const bool prunes = !explorator.searchesLevels() && !walksPendingEdges && (filtersByEndLabels || loopData->getEndNodes() || endNodeSet);

    const bool gatesDistanceIndex = filtersByEndLabels && !loopData->getDistanceIndex()->isBuilt();
    const bool gatesEndSetIndex = endNodeSet && !loopData->getTargetIndex()->isBuilt();
    const bool gatesBoundEndIndex = loopData->getEndNodes() != nullptr;
    const bool readsSample = gatesDistanceIndex || gatesEndSetIndex || gatesBoundEndIndex;

    // Once the walk from the seeds holds its index over the set, it walks only the prefixes
    // that reach the set, as the walk from the set would toward the seeds
    const bool weighsTheEndSet = gatesEndSetIndex && emitsTheSameFromEitherEnd(loopData, walkedBody);

    // What this chunk's own seeds expand to: the gates price the walk by it, and it is what
    // the walk does rather than what the average node carrying the type does
    std::vector<PathStep> body;
    PathDistanceIndex::SeedExpansion expansion;
    const PartDirectory parts(view);
    if (weighsTheEndSet || (prunes && readsSample)) {
        sampledBodyOf(walkedBody, body);
        PathDistanceIndex::sampleSeedExpansion(parts, body, inputNodeIDs->getRaw(), expansion);
    }

    if (weighsTheEndSet) {
        PathDistanceIndex::SeedExpansion fromEnds;
        if (ReversedPathExplorator::isCheaper(parts, body, expansion, inputNodeIDs->size(), endNodes, maxHops, fromEnds)) {
            runFromTheEndSet(context, loopData, body, endNodes, minHops, maxHops, walksPendingEdges, fromEnds);
            return;
        }
    }

    if (prunes) {
        loopData->addSeedsSeen(inputNodeIDs->size());
    }

    if (filtersByEndLabels) {
        explorator.setEndLabels(&loopData->getEndLabels());
        if (prunes) {
            explorator.setDistanceIndex(pruningIndexFor(view, loopData, maxHops, expansion));
        }
    }

    if (loopData->getEndNodes()) {
        explorator.setEndNodes(loopData->getEndNodes());
        if (prunes) {
            explorator.setTargetIndex(targetIndexFor(view, loopData, maxHops, expansion));
        }
    }

    if (endNodeSet) {
        explorator.setEndNodeSet(endNodes);
        if (prunes) {
            explorator.setTargetIndex(endSetTargetIndexFor(view, loopData, endNodes, maxHops, expansion));
        }
    }

    runEdgeLoopSteps(context, loopData, &explorator, nullptr, loopData->getSources());
}

void NLExecutor::runExpandPath(NLExecutionContext* context, NLFunctionData* data) {
    const NLExpandPathData* expand = static_cast<NLExpandPathData*>(data);
    const std::vector<PathRef>& paths = expand->getPaths()->getRaw();
    std::vector<ListView>& lists = expand->getOutput()->getRaw();
    const PathTrie& trie = *expand->getTrie();
    QueryListBuffer& listBuffer = *expand->getListBuffer();
    listBuffer.clear();

    lists.resize(paths.size());

    const bool reversed = expand->isReversed();
    const PathHopStride hops = expand->getHops();

    switch (expand->getKind()) {
        case PathExpansionKind::Edges:
            for (size_t row = 0; row < paths.size(); row++) {
                lists[row] = trie.expandEdges(paths[row], listBuffer, reversed, hops);
            }
        break;

        case PathExpansionKind::Sources: {
            const ColumnNodeIDs* seeds = expand->getSeeds();
            for (size_t row = 0; row < paths.size(); row++) {
                const NodeID seed = seeds ? (*seeds)[row] : NodeID();
                lists[row] = trie.expandSources(paths[row], seed, listBuffer, reversed, hops);
            }
        }
        break;

        case PathExpansionKind::Ends:
            for (size_t row = 0; row < paths.size(); row++) {
                lists[row] = trie.expandEnds(paths[row], listBuffer, reversed, hops);
            }
        break;

        case PathExpansionKind::Nodes: {
            const std::vector<NodeID>& seeds = expand->getSeeds()->getRaw();
            for (size_t row = 0; row < paths.size(); row++) {
                lists[row] = trie.expandNodes(paths[row], seeds[row], listBuffer, reversed);
            }
        }
        break;
    }
}

void NLExecutor::runPathLength(NLExecutionContext* context, NLFunctionData* data) {
    const NLPathLengthData* length = static_cast<NLPathLengthData*>(data);
    const std::vector<PathRef>& paths = length->getPaths()->getRaw();
    std::vector<uint64_t>& lengths = length->getOutput()->getRaw();
    const PathTrie& trie = *length->getTrie();
    const size_t steps = length->getSteps();
    const uint64_t seed = length->countsNodes() ? 1 : 0;

    lengths.resize(paths.size());
    for (size_t row = 0; row < paths.size(); row++) {
        lengths[row] = trie.getDepth(paths[row]) / steps + seed;
    }
}

void NLExecutor::runPathElements(NLExecutionContext* context, NLFunctionData* data) {
    const NLPathElementsData* elements = static_cast<NLPathElementsData*>(data);
    const Column* paths = elements->getPaths();
    const PathElementsKind kind = elements->getKind();
    Column* output = elements->getOutput();
    QueryListBuffer& listBuffer = *elements->getListBuffer();
    listBuffer.clear();

    readPathElements(paths, elements->getRead(), kind, output, listBuffer);
}

void NLExecutor::runMakePath(NLExecutionContext* context, NLFunctionData* data) {
    const NLMakePathData* make = static_cast<NLMakePathData*>(data);
    const NLMakePathData::PathSteps& steps = make->steps();
    std::vector<GraphPath>& paths = make->getOutput()->getRaw();
    const PathTrie& trie = *make->getTrie();

    bioassert(!steps.empty(), "nl.make_path builds a path out of at least one node");

    const NLPathStep& firstStep = steps.front();
    const size_t rowCount = firstStep._nodes ? firstStep._nodes->size() : firstStep._walks->size();

    const auto isRowAligned = [rowCount](const NLPathStep& step) {
        const bool nodesAligned = !step._nodes || step._nodes->size() == rowCount;
        const bool edgesAligned = !step._edges || step._edges->size() == rowCount;
        const bool walksAligned = !step._walks || step._walks->size() == rowCount;

        return nodesAligned && edgesAligned && walksAligned;
    };
    bioassert(std::ranges::all_of(steps, isRowAligned),
              "Entity columns of a path build are not row-aligned.");

    paths.resize(rowCount);
    for (GraphPath& path : paths) {
        path.clear();
    }

    for (const NLPathStep& step : steps) {
        switch (step._kind) {
            case NLPathStep::Kind::Start: {
                const std::vector<NodeID>& nodes = step._nodes->getRaw();
                for (size_t row = 0; row < rowCount; row++) {
                    paths[row].start(nodes[row]);
                }
            }
            break;

            case NLPathStep::Kind::Hop: {
                const std::vector<EdgeID>& edges = step._edges->getRaw();
                const std::vector<NodeID>& nodes = step._nodes->getRaw();
                for (size_t row = 0; row < rowCount; row++) {
                    paths[row].addHop(edges[row], nodes[row]);
                }
            }
            break;

            case NLPathStep::Kind::Walk: {
                const std::vector<PathRef>& walks = step._walks->getRaw();
                for (size_t row = 0; row < rowCount; row++) {
                    trie.appendHops(walks[row], paths[row]);
                }
            }
            break;

            case NLPathStep::Kind::ReversedWalk: {
                const std::vector<PathRef>& walks = step._walks->getRaw();
                const std::vector<NodeID>& seeds = step._nodes->getRaw();
                for (size_t row = 0; row < rowCount; row++) {
                    trie.appendReversedHops(walks[row], seeds[row], paths[row]);
                }
            }
            break;
        }
    }
}

void NLExecutor::runCrossProductLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLCrossProductLoopData* loopData = static_cast<NLCrossProductLoopData*>(data);

    const NLCrossProductLoopData::Columns& outerColumns = loopData->outerColumns();
    const NLCrossProductLoopData::Columns& innerColumns = loopData->innerColumns();
    bioassert(!outerColumns.empty() && !innerColumns.empty(),
              "nl.cross_product needs a column on each side to size the product");

    // N outer rows crossed with M inner rows make N*M pairs. The counts come from
    // the first column of each side; every column of a side is row-aligned with
    // that first one, which is what lets one position slice them all.
    const size_t outerRowCount = outerColumns.front().getInput()->size();
    const size_t innerRowCount = innerColumns.front().getInput()->size();

    const size_t productRowCount = outerRowCount * innerRowCount;
    const size_t chunkSize = context->getChunkSize();
    const NLLimitState* limit = loopData->getLimit();
    const NLStmtContainer* loopBody = loopData->getStmts();

    // Walk the pairs a chunk at a time. Under a limit the step is cut to what the
    // budget can still emit - the pairs come out in (outer, inner) order, so the
    // step's prefix is exactly what the nl.limit_update/nl.output below can take,
    // and the loop stops once the budget is spent, as a scan's loop does.
    size_t position = 0;
    while (position < productRowCount) {
        const size_t remaining = limit ? limit->getRemaining() : productRowCount;
        if (remaining == 0) {
            return;
        }

        const size_t chunkRowCount = std::min(chunkSize, productRowCount - position);
        const size_t rowCount = std::min(chunkRowCount, remaining);

        for (const NLCrossColumn& column : outerColumns) {
            const NLBroadcastFunction broadcast = column.getBroadcast();
            broadcast(column.getInput(), innerRowCount, position, rowCount, column.getOutput());
        }

        for (const NLCrossColumn& column : innerColumns) {
            const NLBroadcastFunction broadcast = column.getBroadcast();
            broadcast(column.getInput(), innerRowCount, position, rowCount, column.getOutput());
        }

        runBody(context, loopBody);

        position += rowCount;
    }
}

void NLExecutor::runLimitInit(NLExecutionContext* context, NLFunctionData* data) {
    const NLLimitInitData* init = static_cast<NLLimitInitData*>(data);
    init->getState()->reset(init->getCount());
}

void NLExecutor::runLimitRemaining(NLExecutionContext* context, NLFunctionData* data) {
    const NLLimitRemainingData* remaining = static_cast<NLLimitRemainingData*>(data);
    remaining->getState()->reset(remaining->getSource()->getRemaining());
}

void NLExecutor::runLimitUpdate(NLExecutionContext* context, NLFunctionData* data) {
    const NLLimitUpdateData* update = static_cast<NLLimitUpdateData*>(data);
    update->getState()->update(update->getRows()->size());
}

void NLExecutor::runLimitTruncate(NLExecutionContext* context, NLFunctionData* data) {
    const NLLimitTruncateData* truncate = static_cast<NLLimitTruncateData*>(data);
    const size_t emitThisStep = truncate->getState()->getEmitThisStep();

    // Block-repeat with factor 1 emits each input row once and stops at
    // emitThisStep, so it copies exactly the prefix [0, emitThisStep) into the
    // fresh output chunk. Reads emitThisStep (set by the preceding
    // nl.limit_update); never mutates the counter.
    for (const NLCrossColumn& column : truncate->columns()) {
        const NLBroadcastFunction copyPrefix = column.getBroadcast();
        copyPrefix(column.getInput(), 1, 0, emitThisStep, column.getOutput());
    }
}

void NLExecutor::runSkipInit(NLExecutionContext* context, NLFunctionData* data) {
    const NLSkipInitData* init = static_cast<NLSkipInitData*>(data);
    init->getState()->reset(init->getCount());
}

void NLExecutor::runSkipUpdate(NLExecutionContext* context, NLFunctionData* data) {
    const NLSkipUpdateData* update = static_cast<NLSkipUpdateData*>(data);
    update->getState()->update(update->getRows()->size());
}

void NLExecutor::runSkipTruncate(NLExecutionContext* context, NLFunctionData* data) {
    const NLSkipTruncateData* truncate = static_cast<NLSkipTruncateData*>(data);
    const NLSkipState* state = truncate->getState();

    // Copy the surviving suffix [skipThisStep, skipThisStep + emitThisStep) of each
    // column into the fresh front-aligned output chunk. Reads the offset and count
    // (set by the preceding nl.skip_update); never mutates the counter.
    const size_t offset = state->getSkipThisStep();
    const size_t rowCount = state->getEmitThisStep();
    for (const NLSkipColumn& column : truncate->columns()) {
        const NLCopyFunction copySuffix = column.getCopy();
        copySuffix(column.getInput(), offset, rowCount, column.getOutput());
    }
}

void NLExecutor::runOutput(NLExecutionContext* context, NLFunctionData* data) {
    NLOutputData* output = static_cast<NLOutputData*>(data);
    const auto& cols = output->outputs();
    bioassert(!cols.empty(), "nl.output requires at least one column");

    // A path chunk reaches the sink as the lists its handles stand for, expanded into a
    // buffer of this emission alone: the sink reads them during the call and never again
    const std::vector<NLOutputData::PathExpansion>& expansions = output->pathExpansions();
    if (!expansions.empty()) {
        QueryListBuffer& listBuffer = output->pathListBuffer();
        listBuffer.clear();

        const PathTrie& trie = *output->getTrie();
        for (const NLOutputData::PathExpansion& expansion : expansions) {
            const std::vector<PathRef>& paths = expansion._paths->getRaw();
            std::vector<ListView>& lists = expansion._lists->getRaw();

            lists.resize(paths.size());
            for (size_t row = 0; row < paths.size(); row++) {
                lists[row] = trie.expandEdges(paths[row], listBuffer, false);
            }
        }
    }

    // Compute the [offset, offset + rowCount) window to emit, copy-free:
    //  - skip (the folded terminal-SKIP form): emit the surviving suffix at offset
    //    getSkipThisStep() for getEmitThisStep() rows, both sized by the preceding
    //    nl.skip_update. Reading them, not remaining, so the decrement already done
    //    does not affect the window.
    //  - limit (the folded terminal-LIMIT form): emit the getEmitThisStep() prefix
    //    nl.limit_update sized this step, from offset zero.
    //  - neither: emit the whole chunk (already cut by a truncate when one governs
    //    it).
    // A folded output carries at most one of limit/skip, so the two never combine.
    const NLLimitState* limit = output->getLimit();
    const NLSkipState* skip = output->getSkip();
    // Column that defines the cardinality of the result
    const Column* cardinality = output->getCardinality();

    size_t offset = 0;
    size_t rowCount = 0;
    if (skip) {
        offset = skip->getSkipThisStep();
        rowCount = skip->getEmitThisStep();
    } else if (limit) {
        rowCount = limit->getEmitThisStep();
    } else if (cardinality) {
        rowCount = cardinality->size();
    } else {
        // Logical row count, assumes the columns that carry rows share a dimension. A
        // constant is not one of them: it holds a single value standing for every row of
        // the step, so a step that kept no row would emit it as a row of its own
        for (const Column* column : output->rowCountColumns()) {
            rowCount = std::max(rowCount, column->size());
        }
    }

    context->getSink()->appendChunks(cols, offset, rowCount);
}

void NLExecutor::runBroadcastConstant(NLExecutionContext*, NLFunctionData* data) {
    const NLBroadcastConstantData* broadcast = static_cast<NLBroadcastConstantData*>(data);
    const Column* cardinality = broadcast->getCardinality();

    // The driving relation's chunk sizes this step, exactly as it sizes an output of
    // constants alone; a projection no relation drives is the single row the constant
    // is. The fill writes that many rows of the constant's value.
    const size_t rowCount = cardinality ? cardinality->size() : 1;

    broadcast->getFill()(broadcast->getValue(), rowCount, broadcast->getOutput());
}

void NLExecutor::runBinary(NLExecutionContext*, NLFunctionData* data) {
    const NLBinaryData* binary = static_cast<NLBinaryData*>(data);
    NLStepBuffers* const stepBuffers = binary->getStepBuffers();
    stepBuffers->clear();

    binary->getFn()(binary->getResult(), binary->getLhs(), binary->getRhs(), stepBuffers);
}

void NLExecutor::runUnary(NLExecutionContext*, NLFunctionData* data) {
    const NLUnaryData* unary = static_cast<NLUnaryData*>(data);
    unary->getFn()(unary->getResult(), unary->getOperand());
}

void NLExecutor::runFillNull(NLExecutionContext*, NLFunctionData* data) {
    const NLFillNullData* fillNull = static_cast<NLFillNullData*>(data);

    fillNull->getFill()(fillNull->getOutput(), fillNull->getRows()->size());
}

void NLExecutor::runCase(NLExecutionContext*, NLFunctionData* data) {
    const NLCaseData* caseData = static_cast<NLCaseData*>(data);

    Column* const result = caseData->getResult();
    const size_t rowCount = caseData->getCardinality()->size();

    NLStepBuffers* const stepBuffers = caseData->getStepBuffers();
    stepBuffers->clear();
    NLValueStore& values = stepBuffers->valueStore();

    caseData->getReset()(result, rowCount);

    const std::vector<NLCaseData::Branch>& branches = caseData->branches();
    const Column* const defaultValue = caseData->getDefaultValue();
    const NLCellWrite& writeDefault = caseData->getWriteDefault();

    // A row takes the first branch whose condition holds, so the branches are walked per
    // row rather than the rows per branch: the first match ends the row
    for (size_t row = 0; row < rowCount; row++) {
        bool matched = false;

        for (const NLCaseData::Branch& branch : branches) {
            if (!branch._test(branch._condition, row)) {
                continue;
            }

            writeCell(result, row, branch._write, branch._value, row, stepBuffers, values);
            matched = true;
            break;
        }

        if (!matched && defaultValue) {
            writeCell(result, row, writeDefault, defaultValue, row, stepBuffers, values);
        }
    }
}

void NLExecutor::runCaseMerge(NLExecutionContext*, NLFunctionData* data) {
    NLCaseData* caseData = static_cast<NLCaseData*>(data);

    Column* const result = caseData->getResult();
    const size_t rowCount = caseData->getCardinality()->size();

    NLStepBuffers* const stepBuffers = caseData->getStepBuffers();
    stepBuffers->clear();
    NLValueStore& values = stepBuffers->valueStore();

    caseData->getReset()(result, rowCount);

    const std::vector<NLCaseData::Branch>& branches = caseData->branches();
    const Column* const defaultValue = caseData->getDefaultValue();
    const NLCellWrite& writeDefault = caseData->getWriteDefault();

    std::vector<NLCaseData::BranchCursor>& cursors = caseData->cursors();
    std::fill(cursors.begin(), cursors.end(), NLCaseData::BranchCursor {});

    size_t defaultRow = 0;

    for (size_t row = 0; row < rowCount; row++) {
        bool matched = false;

        for (size_t branchIndex = 0; branchIndex < branches.size(); branchIndex++) {
            const NLCaseData::Branch& branch = branches[branchIndex];
            NLCaseData::BranchCursor& cursor = cursors[branchIndex];

            const bool holds = branch._test(branch._condition, cursor._conditionRow);
            cursor._conditionRow++;

            if (!holds) {
                continue;
            }

            writeCell(result, row, branch._write, branch._value, cursor._valueRow, stepBuffers, values);
            cursor._valueRow++;
            matched = true;
            break;
        }

        if (!matched && defaultValue) {
            writeCell(result, row, writeDefault, defaultValue, defaultRow, stepBuffers, values);
            defaultRow++;
        }
    }
}

void NLExecutor::runFillNulls(NLExecutionContext*, NLFunctionData* data) {
    const NLFillNullsData* fillNulls = static_cast<NLFillNullsData*>(data);

    fillNulls->getFillNullRows()(fillNulls->getResult(),
                                 fillNulls->getValue(),
                                 fillNulls->getNullRows(),
                                 fillNulls->getFill());
}

void NLExecutor::runMakeList(NLExecutionContext*, NLFunctionData* data) {
    const NLMakeListData* makeList = static_cast<NLMakeListData*>(data);

    const std::vector<NLMakeListData::Element>& elements = makeList->elements();
    NLStepBuffers* const stepBuffers = makeList->getStepBuffers();
    stepBuffers->clear();

    ListBuffer<>& listBuffer = stepBuffers->listBuffer();

    // Every element column is row-aligned with the others, so the first gives the rows
    const size_t rowCount = elements.front()._column->size();

    const auto isRowAligned = [rowCount](const NLMakeListData::Element& element) {
        return element._column->size() == rowCount;
    };
    bioassert(std::ranges::all_of(elements, isRowAligned),
              "Element columns of a list build are not row-aligned.");

    std::vector<ListView>& outputRaw = static_cast<ColumnVector<ListView>*>(makeList->getResult())->getRaw();
    outputRaw.resize(rowCount);

    std::vector<ListBuffer<>::ListItemVariant> row;
    row.reserve(elements.size());

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        row.clear();
        for (const NLMakeListData::Element& element : elements) {
            row.push_back(element._read(element._column, rowIndex, stepBuffers));
        }

        outputRaw[rowIndex] = listBuffer.insert(row);
    }
}

void NLExecutor::runToAny(NLExecutionContext*, NLFunctionData* data) {
    const NLToAnyData* toAny = static_cast<NLToAnyData*>(data);

    NLStepBuffers* const stepBuffers = toAny->getStepBuffers();
    stepBuffers->clear();
    NLValueStore& values = stepBuffers->valueStore();

    const Column* const operand = toAny->getOperand();
    const NLListItemReadFunction read = toAny->getRead();
    const size_t rowCount = operand->size();

    std::vector<CellView>& results = static_cast<ColumnVector<CellView>*>(toAny->getResult())->getRaw();
    results.resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        ListBuffer<>::ListItemVariant item = read(operand, row, stepBuffers);
        results[row] = values.ownCell(item);
    }
}

void NLExecutor::runListSlice(NLExecutionContext*, NLFunctionData* data) {
    const NLListSliceData* slice = static_cast<NLListSliceData*>(data);

    const Column* const list = slice->getList();
    const NLListReadFunction listRead = slice->getListRead();
    const NLListSliceData::Bound& from = slice->getFrom();
    const NLListSliceData::Bound& to = slice->getTo();

    const size_t rowCount = list->size();

    std::vector<std::optional<ListView>>& outputRaw =
        static_cast<ColumnOptVector<ListView>*>(slice->getResult())->getRaw();
    outputRaw.clear();
    outputRaw.reserve(rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<ListView> elements = listRead(list, rowIndex);

        const std::optional<types::Int64::Primitive> lower =
            from._column ? from._read(from._column, rowIndex) : std::optional<types::Int64::Primitive> {0};

        const std::optional<types::Int64::Primitive> upper =
            to._column ? to._read(to._column, rowIndex) : std::optional<types::Int64::Primitive> {};

        const bool boundsPresent = lower.has_value() && (!to._column || upper.has_value());
        if (!elements || !boundsPresent) {
            outputRaw.push_back(std::nullopt);
            continue;
        }

        const int64_t size = static_cast<int64_t>(elements->size());
        const int64_t last = to._column ? *upper : size;

        // A bound counts from the end where it is negative, and is clamped to the list
        // where it runs past either end, so no row errors on a bound of its own
        const int64_t first = std::clamp(*lower < 0 ? size + *lower : *lower, int64_t {0}, size);
        const int64_t end = std::clamp(last < 0 ? size + last : last, int64_t {0}, size);

        const size_t count = end > first ? static_cast<size_t>(end - first) : 0;

        outputRaw.push_back(elements->slice(static_cast<size_t>(first), count));
    }
}

void NLExecutor::runSortMulti(NLExecutionContext*, NLFunctionData* data) {
    const NLSortMultiData* sortMulti = static_cast<NLSortMultiData*>(data);

    const Column* const list = sortMulti->getList();
    const NLListReadFunction listRead = sortMulti->getListRead();
    const Column* const fields = sortMulti->getFields();
    const NLListReadFunction fieldsRead = sortMulti->getFieldsRead();
    const NLRangeBound& limit = sortMulti->getLimit();
    const NLRangeBound& skip = sortMulti->getSkip();

    NLStepBuffers* const stepBuffers = sortMulti->getStepBuffers();
    stepBuffers->clear();

    ListBuffer<>& listBuffer = stepBuffers->listBuffer();

    const size_t rowCount = list->size();

    std::vector<std::optional<ListView>>& outputRaw =
        static_cast<ColumnOptVector<ListView>*>(sortMulti->getResult())->getRaw();
    outputRaw.resize(rowCount);

    std::vector<SortField> sortFields;
    std::span<const CellView> parsedNames;
    std::vector<std::optional<CellView>> sortKeys;
    std::vector<size_t> order;
    std::vector<CellView> sorted;

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<ListView> elements = listRead(list, rowIndex);
        const std::optional<ListView> names = fields ? fieldsRead(fields, rowIndex) : std::optional<ListView> {ListView {}};

        const std::optional<types::Int64::Primitive> kept =
            limit._column ? limit._read(limit._column, rowIndex) : std::optional<types::Int64::Primitive> {-1};

        const std::optional<types::Int64::Primitive> skipped =
            skip._column ? skip._read(skip._column, rowIndex) : std::optional<types::Int64::Primitive> {0};

        const bool readsANull = !elements || !names || !kept || !skipped;
        if (readsANull) {
            outputRaw[rowIndex] = std::nullopt;
            continue;
        } else if (*skipped < 0) {
            throw IRException("coll.sortMulti cannot skip a negative count");
        }

        const std::span<const CellView> nameElements = names->elements();
        const bool namesOtherFields = nameElements.data() != parsedNames.data() || nameElements.size() != parsedNames.size();
        if (namesOtherFields) {
            readSortFields(*names, sortFields);
            parsedNames = nameElements;
        }

        const std::span<const CellView> maps = elements->elements();
        const size_t size = maps.size();
        const size_t fieldCount = sortFields.size();

        const size_t first = std::min(size, static_cast<size_t>(*skipped));
        const size_t remaining = size - first;
        const size_t count = *kept < 0 ? remaining : std::min(remaining, static_cast<size_t>(*kept));

        if (fieldCount == 0) {
            outputRaw[rowIndex] = elements->slice(first, count);
            continue;
        }

        sortKeys.resize(size * fieldCount);

        for (size_t position = 0; position < size; position++) {
            const CellView element = maps[position];
            const CellTag tag = element.getTag();
            const std::span<std::optional<CellView>> keysOfElement {sortKeys.data() + position * fieldCount, fieldCount};

            // A key read off null is null, as `null.a` is
            if (tag == CellTag::Null) {
                std::ranges::fill(keysOfElement, std::nullopt);
                continue;
            } else if (tag != CellTag::MapView) {
                throw IRException(fmt::format("coll.sortMulti sorts a list of maps, not of '{}'", queryTypeName(tag)));
            }

            const MapView map = element.getAs<MapView>();
            for (size_t field = 0; field < fieldCount; field++) {
                keysOfElement[field] = sortKey(map, sortFields[field]._key);
            }
        }

        order.resize(size);
        std::iota(order.begin(), order.end(), size_t {0});

        const std::span<const std::optional<CellView>> keys {sortKeys};
        std::ranges::stable_sort(order, [&keys, &sortFields, fieldCount](size_t lhs, size_t rhs) {
            return orderOnKeys(keys.subspan(lhs * fieldCount, fieldCount),
                               keys.subspan(rhs * fieldCount, fieldCount),
                               sortFields) < 0;
        });

        sorted.resize(count);
        for (size_t position = 0; position < count; position++) {
            sorted[position] = maps[order[first + position]];
        }

        outputRaw[rowIndex] = listBuffer.gather(sorted);
    }
}

void NLExecutor::runPercentileCont(NLExecutionContext*, NLFunctionData* data) {
    const auto interpolate = [](const std::vector<CellView>& sorted, double percentile) {
        return std::optional<double>(continuousPercentile(sorted, percentile));
    };

    runPercentileRows<std::optional<double>>(static_cast<NLPercentileData*>(data), "percentileCont", interpolate);
}

template <typename Result>
void NLExecutor::runPercentileDisc(NLExecutionContext*, NLFunctionData* data) {
    const auto pick = [](const std::vector<CellView>& sorted, double percentile) {
        return percentileElement<Result>(sorted[discretePercentilePosition(sorted.size(), percentile)]);
    };

    runPercentileRows<Result>(static_cast<NLPercentileData*>(data), "percentileDisc", pick);
}

template void NLExecutor::runPercentileDisc<std::optional<types::Int64::Primitive>>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runPercentileDisc<std::optional<types::UInt64::Primitive>>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runPercentileDisc<std::optional<types::Double::Primitive>>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runPercentileDisc<CellView>(NLExecutionContext* context, NLFunctionData* data);

void NLExecutor::runMakeMap(NLExecutionContext*, NLFunctionData* data) {
    const NLMakeMapData* makeMap = static_cast<NLMakeMapData*>(data);

    const std::vector<NLMakeMapData::Entry>& entries = makeMap->entries();
    NLStepBuffers* memory = makeMap->getStepBuffers();
    memory->clear();
    MapBuffer<>& mapBuffer = memory->mapBuffer();

    const size_t rowCount = entries.front()._column->size();

    const auto isRowAligned = [rowCount](const NLMakeMapData::Entry& entry) {
        return entry._column->size() == rowCount;
    };
    bioassert(std::ranges::all_of(entries, isRowAligned),
              "Value columns of a map build are not row-aligned.");

    std::vector<MapView>& outputRaw = static_cast<ColumnVector<MapView>*>(makeMap->getResult())->getRaw();
    outputRaw.resize(rowCount);

    std::vector<MapBuffer<>::MapKeyValuePair> row;
    row.reserve(entries.size());

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        row.clear();
        for (const NLMakeMapData::Entry& entry : entries) {
            row.push_back(MapBuffer<>::MapKeyValuePair {
                .key = entry._key,
                .value = entry._read(entry._column, rowIndex, memory),
            });
        }

        outputRaw[rowIndex] = mapBuffer.insert(row);
    }
}

template <typename Result>
void NLExecutor::runMergeMaps(NLExecutionContext*, NLFunctionData* data) {
    const NLMergeMapsData* merge = static_cast<NLMergeMapsData*>(data);

    const std::vector<NLMergeMapsData::Operand>& operands = merge->getOperands();
    NLStepBuffers* memory = merge->getStepBuffers();
    memory->clear();
    MapBuffer<>& mapBuffer = memory->mapBuffer();

    const size_t rowCount = operands.front()._column->size();

    const auto isRowAligned = [rowCount](const NLMergeMapsData::Operand& operand) {
        return operand._column->size() == rowCount;
    };
    bioassert(std::ranges::all_of(operands, isRowAligned),
              "Map columns of a merge are not row-aligned.");

    std::vector<Result>& outputRaw = static_cast<ColumnVector<Result>*>(merge->getResult())->getRaw();
    outputRaw.resize(rowCount);

    std::vector<MapEntryView> entries;
    std::vector<size_t> order;
    std::vector<MapBuffer<>::MapKeyValuePair> merged;

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        entries.clear();

        bool holdsEveryMap = true;
        for (const NLMergeMapsData::Operand& operand : operands) {
            const NLKeyedValue keyed = operand._read(operand._column, rowIndex);
            const MapView* map = std::get_if<MapView>(&keyed);

            if (!map) {
                holdsEveryMap = false;
                break;
            }

            entries.insert(entries.end(), map->begin(), map->end());
        }

        if constexpr (TypeUtils::is_optional_v<Result>) {
            if (!holdsEveryMap) {
                outputRaw[rowIndex] = std::nullopt;
                continue;
            }
        } else {
            bioassert(holdsEveryMap, "A merge typed as never null read a row holding no map.");
        }

        // Equal keys keep their operand order, so the last of each run is the one that stays
        order.resize(entries.size());
        std::iota(order.begin(), order.end(), 0);
        std::ranges::sort(order, [&entries](size_t lhs, size_t rhs) {
            const std::string_view lhsKey = entries[lhs].getKey();
            const std::string_view rhsKey = entries[rhs].getKey();

            return lhsKey < rhsKey || (lhsKey == rhsKey && lhs < rhs);
        });

        merged.clear();
        for (size_t position = 0; position < order.size(); position++) {
            const MapEntryView entry = entries[order[position]];

            const bool replacedLater = position + 1 < order.size()
                                    && entries[order[position + 1]].getKey() == entry.getKey();
            if (replacedLater) {
                continue;
            }

            merged.push_back(MapBuffer<>::MapKeyValuePair {
                .key = entry.getKey(),
                .value = taggedItem<MapBuffer<>::MapItemVariant>(entry.getValue()),
            });
        }

        outputRaw[rowIndex] = mapBuffer.insert(merged);
    }
}

template void NLExecutor::runMergeMaps<MapView>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runMergeMaps<std::optional<MapView>>(NLExecutionContext* context, NLFunctionData* data);

template <typename Result>
void NLExecutor::runSubstring(NLExecutionContext*, NLFunctionData* data) {
    const NLSubstringData* substring = static_cast<NLSubstringData*>(data);

    const NLSubstringData::StringArgument& string = substring->getString();
    const NLSubstringData::IntegerArgument& start = substring->getStart();
    const NLSubstringData::IntegerArgument& length = substring->getLength();

    const size_t rowCount = string._column->size();
    bioassert(start._column->size() == rowCount, "Start column of a substring is not row-aligned with its string.");
    bioassert(!length._column || length._column->size() == rowCount,
              "Length column of a substring is not row-aligned with its string.");

    std::vector<Result>& outputRaw = static_cast<ColumnVector<Result>*>(substring->getResult())->getRaw();
    outputRaw.resize(rowCount);

    const SubstringFunction function {};
    constexpr std::string_view name = SubstringFunction::NAME;

    constexpr std::optional<types::Int64::Primitive> toTheEnd = MAX_CHARACTER_ARGUMENT;

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<types::String::Primitive> text = string._read(string._column, rowIndex, name);
        const std::optional<types::Int64::Primitive> from = start._read(start._column, rowIndex, name);
        const std::optional<types::Int64::Primitive> count = length._column ? length._read(length._column, rowIndex, name) : toTheEnd;

        const bool boundIsNull = !from.has_value() || !count.has_value();
        if (!text.has_value()) {
            if constexpr (TypeUtils::is_optional_v<Result>) {
                outputRaw[rowIndex] = std::nullopt;
            } else {
                throw IRException("substring() typed as never null read a null string");
            }
        } else if (boundIsNull) {
            throw IRException("substring() does not take a null start or length");
        } else {
            outputRaw[rowIndex] = function(*text, *from, *count);
        }
    }
}

template void NLExecutor::runSubstring<types::String::Primitive>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runSubstring<std::optional<types::String::Primitive>>(NLExecutionContext* context, NLFunctionData* data);

template <typename Result>
void NLExecutor::runSplit(NLExecutionContext*, NLFunctionData* data) {
    const NLSplitData* split = static_cast<NLSplitData*>(data);

    const NLSplitData::StringArgument& string = split->getString();
    const NLSplitData::DelimitersArgument& delimiters = split->getDelimiters();

    const size_t rowCount = string._column->size();
    bioassert(delimiters._column->size() == rowCount, "Delimiter column of a split is not row-aligned with its string.");

    std::vector<Result>& outputRaw = static_cast<ColumnVector<Result>*>(split->getResult())->getRaw();
    outputRaw.resize(rowCount);

    NLStepBuffers* const stepBuffers = split->getStepBuffers();
    stepBuffers->clear();

    SplitFunction function(&stepBuffers->listBuffer());
    constexpr std::string_view name = SplitFunction::NAME;

    std::vector<types::String::Primitive> separators;

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<types::String::Primitive> text = string._read(string._column, rowIndex, name);
        const bool readsSeparators = delimiters._read(delimiters._column, rowIndex, name, separators);

        const bool readsANull = !text.has_value() || !readsSeparators;
        if (!readsANull) {
            outputRaw[rowIndex] = function(*text, separators);
        } else if constexpr (TypeUtils::is_optional_v<Result>) {
            outputRaw[rowIndex] = std::nullopt;
        } else {
            throw IRException("split() typed as never null read a null argument");
        }
    }
}

template void NLExecutor::runSplit<ListView>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runSplit<std::optional<ListView>>(NLExecutionContext* context, NLFunctionData* data);

template <typename Function, typename Result>
void NLExecutor::runStringEnd(NLExecutionContext*, NLFunctionData* data) {
    const NLStringEndData* stringEnd = static_cast<NLStringEndData*>(data);

    const NLStringEndData::StringArgument& string = stringEnd->getString();
    const NLStringEndData::IntegerArgument& length = stringEnd->getLength();

    const size_t rowCount = string._column->size();
    bioassert(length._column->size() == rowCount, "Length column of a string end is not row-aligned with its string.");

    std::vector<Result>& outputRaw = static_cast<ColumnVector<Result>*>(stringEnd->getResult())->getRaw();
    outputRaw.resize(rowCount);

    const Function function {};
    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<types::String::Primitive> text = string._read(string._column, rowIndex, Function::NAME);
        const std::optional<types::Int64::Primitive> count = length._read(length._column, rowIndex, Function::NAME);

        if (!text.has_value()) {
            if constexpr (TypeUtils::is_optional_v<Result>) {
                outputRaw[rowIndex] = std::nullopt;
            } else {
                throw IRException(fmt::format("{}() typed as never null read a null string", Function::NAME));
            }
        } else if (!count.has_value()) {
            throw IRException(fmt::format("{}() does not take a null length", Function::NAME));
        } else {
            outputRaw[rowIndex] = function(*text, *count);
        }
    }
}

template void NLExecutor::runStringEnd<LeftFunction, types::String::Primitive>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runStringEnd<LeftFunction, std::optional<types::String::Primitive>>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runStringEnd<RightFunction, types::String::Primitive>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runStringEnd<RightFunction, std::optional<types::String::Primitive>>(NLExecutionContext* context, NLFunctionData* data);

template <typename Result>
void NLExecutor::runReplace(NLExecutionContext*, NLFunctionData* data) {
    const NLReplaceData* replace = static_cast<NLReplaceData*>(data);

    const NLReplaceData::StringArgument& string = replace->getString();
    const NLReplaceData::StringArgument& search = replace->getSearch();
    const NLReplaceData::StringArgument& replacement = replace->getReplacement();

    const size_t rowCount = string._column->size();
    bioassert(search._column->size() == rowCount, "Search column of a replace is not row-aligned with its string.");
    bioassert(replacement._column->size() == rowCount, "Replacement column of a replace is not row-aligned with its string.");

    std::vector<Result>& outputRaw = static_cast<ColumnVector<Result>*>(replace->getResult())->getRaw();
    outputRaw.resize(rowCount);

    NLStepBuffers* const stepBuffers = replace->getStepBuffers();
    stepBuffers->clear();

    ReplaceFunction function(&stepBuffers->stringBuffer());
    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<types::String::Primitive> text = string._read(string._column, rowIndex, ReplaceFunction::NAME);
        const std::optional<types::String::Primitive> searched = search._read(search._column, rowIndex, ReplaceFunction::NAME);
        const std::optional<types::String::Primitive> replacing = replacement._read(replacement._column, rowIndex, ReplaceFunction::NAME);

        const bool readsANull = !text.has_value() || !searched.has_value() || !replacing.has_value();
        if (!readsANull) {
            outputRaw[rowIndex] = function(*text, *searched, *replacing);
        } else if constexpr (TypeUtils::is_optional_v<Result>) {
            outputRaw[rowIndex] = std::nullopt;
        } else {
            throw IRException("replace() typed as never null read a null argument");
        }
    }
}

template void NLExecutor::runReplace<types::String::Primitive>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runReplace<std::optional<types::String::Primitive>>(NLExecutionContext* context, NLFunctionData* data);

void NLExecutor::runPointDistance(NLExecutionContext*, NLFunctionData* data) {
    const NLPointFunctionData* function = static_cast<NLPointFunctionData*>(data);

    const std::vector<NLPointFunctionData::PointArgument>& arguments = function->getArguments();
    const NLPointFunctionData::PointArgument& lhs = arguments[0];
    const NLPointFunctionData::PointArgument& rhs = arguments[1];

    const size_t rowCount = lhs._column->size();
    bioassert(rhs._column->size() == rowCount, "Columns of a point.distance are not row-aligned.");

    std::vector<std::optional<types::Double::Primitive>>& outputRaw =
        static_cast<ColumnOptVector<types::Double::Primitive>*>(function->getResult())->getRaw();
    outputRaw.resize(rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<Point> from = lhs._read(lhs._column, rowIndex, "point.distance");
        const std::optional<Point> to = rhs._read(rhs._column, rowIndex, "point.distance");

        if (from.has_value() && to.has_value()) {
            outputRaw[rowIndex] = Point::distance(*from, *to);
        } else {
            outputRaw[rowIndex] = std::nullopt;
        }
    }
}

void NLExecutor::runPointWithinBBox(NLExecutionContext*, NLFunctionData* data) {
    const NLPointFunctionData* function = static_cast<NLPointFunctionData*>(data);

    const std::vector<NLPointFunctionData::PointArgument>& arguments = function->getArguments();
    const NLPointFunctionData::PointArgument& point = arguments[0];
    const NLPointFunctionData::PointArgument& lowerLeft = arguments[1];
    const NLPointFunctionData::PointArgument& upperRight = arguments[2];

    const size_t rowCount = point._column->size();
    bioassert(lowerLeft._column->size() == rowCount, "Columns of a point.withinBBox are not row-aligned.");
    bioassert(upperRight._column->size() == rowCount, "Columns of a point.withinBBox are not row-aligned.");

    std::vector<std::optional<types::Bool::Primitive>>& outputRaw =
        static_cast<ColumnOptVector<types::Bool::Primitive>*>(function->getResult())->getRaw();
    outputRaw.resize(rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<Point> value = point._read(point._column, rowIndex, "point.withinBBox");
        const std::optional<Point> lower = lowerLeft._read(lowerLeft._column, rowIndex, "point.withinBBox");
        const std::optional<Point> upper = upperRight._read(upperRight._column, rowIndex, "point.withinBBox");

        const bool readsANull = !value.has_value() || !lower.has_value() || !upper.has_value();
        const std::optional<bool> within = readsANull ? std::nullopt : Point::withinBBox(*value, *lower, *upper);

        if (within.has_value()) {
            outputRaw[rowIndex] = types::Bool::Primitive {*within};
        } else {
            outputRaw[rowIndex] = std::nullopt;
        }
    }
}

void NLExecutor::runRange(NLExecutionContext*, NLFunctionData* data) {
    const NLRangeData* range = static_cast<NLRangeData*>(data);

    const NLRangeData::Bound& start = range->getStart();
    const NLRangeData::Bound& end = range->getEnd();
    const NLRangeData::Bound& step = range->getStep();

    NLStepBuffers* const stepBuffers = range->getStepBuffers();
    stepBuffers->clear();

    ListBuffer<>& listBuffer = stepBuffers->listBuffer();

    const size_t rowCount = start._column->size();
    bioassert(end._column->size() == rowCount, "Bound columns of a range are not row-aligned.");
    bioassert(!step._column || step._column->size() == rowCount,
              "Step column of a range is not row-aligned with its bounds.");

    std::vector<std::optional<ListView>>& outputRaw =
        static_cast<ColumnOptVector<ListView>*>(range->getResult())->getRaw();
    outputRaw.clear();
    outputRaw.reserve(rowCount);

    std::vector<ListBuffer<>::ListItemVariant> elements;
    NLRangeSpan span;

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (!span.read(start, end, step, rowIndex)) {
            outputRaw.push_back(std::nullopt);
            continue;
        }

        elements.clear();

        if (!span.isEmpty()) {
            const uint64_t steps = span.getSteps();

            if (steps >= RANGE_LENGTH_LIMIT) {
                throw IRException(fmt::format("range() size exceeds {} integers", RANGE_LENGTH_LIMIT));
            }

            const uint64_t length = steps + 1;

            elements.reserve(length);

            for (uint64_t position = 0; position < length; position++) {
                elements.push_back(ListBuffer<>::ListItemVariant {span.at(position)});
            }
        }

        outputRaw.push_back(listBuffer.insert(elements));
    }
}

void NLExecutor::runRowListsReset(NLExecutionContext* context, NLFunctionData* data) {
    NLRowListsResetData* reset = static_cast<NLRowListsResetData*>(data);

    // No cardinality chunk: nothing was in flight where the lists are read, which is the
    // single empty row Cypher starts from
    const Column* const cardinality = reset->getCardinality();
    const size_t rowCount = cardinality ? cardinality->size() : 1;

    reset->getState()->reset(rowCount);

    // The tag holds each row's position, so the body carries it the way it carries any
    // other column and the collect reads the row each value belongs to off it.
    std::vector<uint64_t>& tagRaw = reset->getTag()->getRaw();
    tagRaw.resize(rowCount);
    std::iota(tagRaw.begin(), tagRaw.end(), uint64_t {0});
}

void NLExecutor::runRowListsCollect(NLExecutionContext* context, NLFunctionData* data) {
    NLRowListsCollectData* collect = static_cast<NLRowListsCollectData*>(data);

    NLRowListsState* state = collect->getState();
    const Column* const value = collect->getValue();
    const NLListItemReadFunction valueRead = collect->getValueRead();
    NLStepBuffers* const memory = collect->getStepBuffers();
    memory->clear();

    const ColumnVector<uint64_t>* const tag = collect->getTag();

    if (!tag) {
        const uint64_t row = untaggedRow(collect->getRow());

        for (size_t match = 0; match < value->size(); match++) {
            state->stage(row, valueRead(value, match, memory));
        }

        return;
    }

    const std::vector<uint64_t>& tagRaw = tag->getRaw();

    bioassert(value->size() == tagRaw.size(),
              "Yielded value of a row lists body is not row-aligned with its row tags.");

    for (size_t match = 0; match < tagRaw.size(); match++) {
        state->stage(tagRaw[match], valueRead(value, match, memory));
    }
}

void NLExecutor::runRowListsResult(NLExecutionContext* context, NLFunctionData* data) {
    NLRowListsResultData* result = static_cast<NLRowListsResultData*>(data);

    NLStepBuffers* const memory = result->getStepBuffers();
    memory->clear();

    result->getState()->buildLists(memory->listBuffer(), result->getResult()->getRaw());
}

void NLExecutor::runListPredicate(NLExecutionContext* context, NLFunctionData* data) {
    NLListPredicateData* predicate = static_cast<NLListPredicateData*>(data);

    const Column* source = predicate->getSource();

    const NLCellAbsentFunction cellAbsent = predicate->getCellAbsentFunc();
    const NLTruthReadFunction truthRead = predicate->getTruthRead();
    const Column* value = predicate->getValue();

    NLElementCursor cursor(source, predicate->getElementCountFunc());
    const size_t sourceRows = cursor.getSourceRows();

    std::vector<size_t>& trueCounts = predicate->trueCounts();
    std::vector<size_t>& falseCounts = predicate->falseCounts();
    std::vector<size_t>& nullCounts = predicate->nullCounts();
    trueCounts.assign(sourceRows, 0);
    falseCounts.assign(sourceRows, 0);
    nullCounts.assign(sourceRows, 0);

    const ListPredicateKind kind = predicate->getKind();

    runElementBody(context, predicate, cursor, [&](const std::vector<uint64_t>& taggedRaw) {
        for (size_t element = 0; element < taggedRaw.size(); element++) {
            const std::optional<bool> truth = truthRead(value, element);
            const size_t row = taggedRaw[element];

            if (!truth.has_value()) {
                nullCounts[row]++;
            } else if (*truth) {
                trueCounts[row]++;
            } else {
                falseCounts[row]++;
            }
        }

        if (!cursor.exhausted()) {
            const size_t openRow = cursor.getRow();
            if (isListPredicateDecided(kind, trueCounts[openRow], falseCounts[openRow])) {
                cursor.skipRow();
            }
        }
    });

    ColumnOptVector<CustomBool>* result = static_cast<ColumnOptVector<CustomBool>*>(predicate->getResult());
    result->resize(sourceRows);

    for (size_t row = 0; row < sourceRows; row++) {
        if (cellAbsent(source, row)) {
            result->setNull(row);
            continue;
        }

        const std::optional<bool> decided = decideListPredicate(kind, trueCounts[row], falseCounts[row], nullCounts[row]);
        if (decided.has_value()) {
            result->set(row, CustomBool(*decided));
        } else {
            result->setNull(row);
        }
    }
}

void NLExecutor::runListComprehension(NLExecutionContext* context, NLFunctionData* data) {
    NLListComprehensionData* comprehension = static_cast<NLListComprehensionData*>(data);

    const Column* source = comprehension->getSource();

    const NLCellAbsentFunction cellAbsent = comprehension->getCellAbsentFunc();
    const NLListItemReadFunction valueRead = comprehension->getValueRead();
    const Column* value = comprehension->getValue();
    NLStepBuffers* const memory = comprehension->getStepBuffers();
    memory->clear();
    ListBuffer<>& listBuffer = memory->listBuffer();

    NLElementCursor cursor(source, comprehension->getElementCountFunc());
    const size_t sourceRows = cursor.getSourceRows();

    // What the rows the cursor still has open gathered: the elements arrive in row order,
    // so a row's elements are one contiguous run of this buffer and the counts locate it
    std::vector<ListBuffer<>::ListItemVariant>& staged = comprehension->stagedElements();
    std::vector<size_t>& stagedCounts = comprehension->stagedCounts();
    NLValueStore& stagedValues = comprehension->stagedValues();
    staged.clear();
    stagedValues.clear();
    stagedCounts.assign(sourceRows, 0);

    std::vector<std::optional<ListView>>& resultRaw =
        static_cast<ColumnOptVector<ListView>*>(comprehension->getResult())->getRaw();
    resultRaw.clear();
    resultRaw.reserve(sourceRows);

    // A row's list is complete as soon as the run past it starts, so the rows behind the
    // cursor are built and dropped at every step rather than the whole source being held
    const auto buildRowsBefore = [&](size_t end) {
        size_t builtElements = 0;

        while (resultRaw.size() < end) {
            const size_t row = resultRaw.size();
            const size_t rowElements = stagedCounts[row];
            const std::span<const ListBuffer<>::ListItemVariant> elements {staged.data() + builtElements,
                                                                          rowElements};

            // A cell holding no list gives its row no list either, which is what Cypher
            // reads `[x IN null | x]` as - as opposed to the empty list a cell whose
            // elements the predicate all dropped gives
            if (cellAbsent(source, row)) {
                resultRaw.push_back(std::nullopt);
            } else {
                resultRaw.push_back(listBuffer.insert(elements));
            }

            builtElements += rowElements;
        }

        staged.erase(staged.begin(), staged.begin() + builtElements);
    };

    runElementBody(context, comprehension, cursor, [&](const std::vector<uint64_t>& keptRaw) {
        for (size_t element = 0; element < keptRaw.size(); element++) {
            staged.push_back(stagedValues.ownItem(valueRead(value, element, memory)));
            stagedCounts[keptRaw[element]]++;
        }

        buildRowsBefore(cursor.getRow());
    });

    buildRowsBefore(sourceRows);
}

void NLExecutor::runReduce(NLExecutionContext* context, NLFunctionData* data) {
    NLFoldData* fold = static_cast<NLFoldData*>(data);

    foldElements(context, fold, [](std::vector<size_t>& endedRows) {
    });
}

void NLExecutor::runAllReduce(NLExecutionContext* context, NLFunctionData* data) {
    NLAllReduceData* allReduce = static_cast<NLAllReduceData*>(data);

    const Column* source = allReduce->getSource();
    const NLCellAbsentFunction cellAbsent = allReduce->getCellAbsentFunc();
    ColumnVector<size_t>* rows = allReduce->getRows();
    const Column* accumulator = allReduce->getAccumulator();
    Column* testedAccumulator = allReduce->getTestedAccumulator();
    const NLGatherFunction accumulatorGather = allReduce->getAccumulatorGather();
    const NLStmtContainer* predicate = allReduce->getPredicateStmts();
    const NLTruthReadFunction truthRead = allReduce->getTruthRead();
    const Column* holds = allReduce->getHolds();

    const size_t sourceRows = source->size();

    std::vector<bool>& failedRows = allReduce->failedRows();
    std::vector<bool>& unknownRows = allReduce->unknownRows();
    failedRows.assign(sourceRows, false);
    unknownRows.assign(sourceRows, false);

    foldElements(context, allReduce, [&](std::vector<size_t>& endedRows) {
        accumulatorGather(accumulator, rows, testedAccumulator);
        runBody(context, predicate);

        const std::vector<size_t>& rowsRaw = rows->getRaw();

        bioassert(holds->size() == rowsRaw.size(),
                  "Yielded predicate of an nl.all_reduce is not row-aligned with its elements.");

        for (size_t index = 0; index < rowsRaw.size(); index++) {
            const size_t row = rowsRaw[index];
            const std::optional<bool> truth = truthRead(holds, index);

            if (!truth.has_value()) {
                unknownRows[row] = true;
            } else if (!*truth) {
                failedRows[row] = true;
                endedRows.push_back(row);
            }
        }
    });

    ColumnOptVector<CustomBool>* result = static_cast<ColumnOptVector<CustomBool>*>(allReduce->getResult());
    result->resize(sourceRows);

    for (size_t row = 0; row < sourceRows; row++) {
        if (cellAbsent(source, row)) {
            result->setNull(row);
        } else if (failedRows[row]) {
            result->set(row, CustomBool(false));
        } else if (unknownRows[row]) {
            result->setNull(row);
        } else {
            result->set(row, CustomBool(true));
        }
    }
}

NLCaseResetFn NLExecutor::selectTaggedCellReset() {
    return &resetTaggedCells;
}

NLCaseWriteFn NLExecutor::selectNullCellWrite() {
    return &caseWriteNullTaggedCell;
}

NLListItemReadFunction NLExecutor::selectValueListItemRead(NLChunkKind kind) {
    NLListItemReadFunction selected = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { selected = &valueItem<ListBuffer<>::ListItemVariant, Element>; });

    return selected;
}

NLKeyedReadFunction NLExecutor::selectKeyedRead(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnVector<MapView>::staticKind()) {
        return &mapRead<ColumnVector<MapView>>;
    } else if (kind == ColumnOptVector<MapView>::staticKind()) {
        return &mapRead<ColumnOptVector<MapView>>;
    } else if (kind == ColumnConst<MapView>::staticKind()) {
        return &mapRead<ColumnConst<MapView>>;
    } else if (kind == ColumnConst<std::optional<MapView>>::staticKind()) {
        return &mapRead<ColumnConst<std::optional<MapView>>>;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &readTaggedKeyed<ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &readTaggedKeyed<ColumnConst<CellView>>;
    }

    throw IRException("a map key read reads a map column, or a column of tagged cells");
}

NLStringReadFunction NLExecutor::selectStringRead(const Column* input) {
    using String = types::String::Primitive;

    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnVector<String>::staticKind()) {
        return &stringRead<ColumnVector<String>>;
    } else if (kind == ColumnOptVector<String>::staticKind()) {
        return &stringRead<ColumnOptVector<String>>;
    } else if (kind == ColumnConst<String>::staticKind()) {
        return &stringRead<ColumnConst<String>>;
    } else if (kind == ColumnConst<std::optional<String>>::staticKind()) {
        return &stringRead<ColumnConst<std::optional<String>>>;
    } else if (kind == ColumnConst<PropertyNull>::staticKind()) {
        return &nullStringRead;
    }

    throw IRException("a dynamic map key read reads its key out of a string column");
}

NLStringArgumentRead NLExecutor::selectStringArgumentRead(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnOptVector<types::String::Primitive>::staticKind()) {
        return &stringValueArgument<ColumnOptVector<types::String::Primitive>>;
    } else if (kind == ColumnConst<types::String::Primitive>::staticKind()) {
        return &stringValueArgument<ColumnConst<types::String::Primitive>>;
    } else if (kind == ColumnConst<std::optional<types::String::Primitive>>::staticKind()) {
        return &stringValueArgument<ColumnConst<std::optional<types::String::Primitive>>>;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &cellStringArgument<ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &cellStringArgument<ColumnConst<CellView>>;
    }

    throw IRException("a string function reads a string argument out of a nullable string column, or a column of tagged cells");
}

NLStringListArgumentRead NLExecutor::selectStringListArgumentRead(const Column* input) {
    using String = types::String::Primitive;

    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnOptVector<String>::staticKind()) {
        return &stringAsStringListArgument<ColumnOptVector<String>>;
    } else if (kind == ColumnConst<String>::staticKind()) {
        return &stringAsStringListArgument<ColumnConst<String>>;
    } else if (kind == ColumnConst<std::optional<String>>::staticKind()) {
        return &stringAsStringListArgument<ColumnConst<std::optional<String>>>;
    } else if (kind == ColumnVector<ListView>::staticKind()) {
        return &listAsStringListArgument<ColumnVector<ListView>>;
    } else if (kind == ColumnOptVector<ListView>::staticKind()) {
        return &listAsStringListArgument<ColumnOptVector<ListView>>;
    } else if (kind == ColumnConst<ListView>::staticKind()) {
        return &listAsStringListArgument<ColumnConst<ListView>>;
    } else if (kind == ColumnConst<std::optional<ListView>>::staticKind()) {
        return &listAsStringListArgument<ColumnConst<std::optional<ListView>>>;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &cellAsStringListArgument<ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &cellAsStringListArgument<ColumnConst<CellView>>;
    }

    throw IRException("a string function reads a list of strings out of a nullable string column, a list column, or a column of tagged cells");
}

NLPointArgumentRead NLExecutor::selectPointArgumentRead(const Column* input) {
    using PointValue = types::Point::Primitive;

    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnOptVector<PointValue>::staticKind()) {
        return &pointValueArgument<ColumnOptVector<PointValue>>;
    } else if (kind == ColumnConst<PointValue>::staticKind()) {
        return &pointValueArgument<ColumnConst<PointValue>>;
    } else if (kind == ColumnConst<std::optional<PointValue>>::staticKind()) {
        return &pointValueArgument<ColumnConst<std::optional<PointValue>>>;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &cellPointArgument<ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &cellPointArgument<ColumnConst<CellView>>;
    }

    throw IRException("a point function reads a point argument out of a nullable point column, or a column of tagged cells");
}

NLIntegerArgumentRead NLExecutor::selectIntegerArgumentRead(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnOptVector<types::Int64::Primitive>::staticKind()) {
        return &integerValueArgument;
    } else if (kind == ColumnOptVector<types::UInt64::Primitive>::staticKind()) {
        return &unsignedValueArgument;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &cellIntegerArgument;
    }

    throw IRException("a string function reads an integer argument out of a nullable integer column, or a column of tagged cells");
}

// Row r of the result views the value row r's map already holds under the key, so the read
// copies nothing. A null row, and a map holding no such key, read back as a null cell.
void NLExecutor::runStaticMapKey(NLExecutionContext* context, NLFunctionData* data) {
    const NLStaticMapKeyData* mapKey = static_cast<NLStaticMapKeyData*>(data);

    const Column* input = mapKey->getInput();
    const NLKeyedReadFunction read = mapKey->getKeyedRead();

    NLStepBuffers* const stepBuffers = mapKey->getStepBuffers();
    stepBuffers->clear();

    MapBuffer<>& mapBuffer = stepBuffers->mapBuffer();
    NLEntityProperties properties(context, &stepBuffers->valueStore());

    const auto valueAt = [input, read, mapKey, &properties, &mapBuffer](size_t row) {
        return readStaticKey(read(input, row), mapKey, properties, mapBuffer);
    };

    Column* result = mapKey->getResult();

    if (result->getKind() == ColumnConst<CellView>::staticKind()) {
        static_cast<ColumnConst<CellView>*>(result)->set(valueAt(0));
        return;
    }

    std::vector<CellView>& outputRaw = static_cast<ColumnVector<CellView>*>(result)->getRaw();
    const size_t rowCount = input->size();

    outputRaw.resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        outputRaw[row] = valueAt(row);
    }
}

// Row r of the result views the value row r's map already holds under row r's key, so the
// read copies nothing. A map holding no such key, and a row holding no key, read back as a
// null cell.
void NLExecutor::runDynamicMapKey(NLExecutionContext* context, NLFunctionData* data) {
    const NLDynamicMapKeyData* dynamicMapKey = static_cast<NLDynamicMapKeyData*>(data);

    const Column* input = dynamicMapKey->getInput();
    const Column* keyColumn = dynamicMapKey->getKey();

    const NLKeyedReadFunction readKeyed = dynamicMapKey->getKeyedRead();
    const NLStringReadFunction readKey = dynamicMapKey->getKeyRead();

    NLStepBuffers* const stepBuffers = dynamicMapKey->getStepBuffers();
    stepBuffers->clear();

    MapBuffer<>& mapBuffer = stepBuffers->mapBuffer();
    NLEntityProperties properties(context, &stepBuffers->valueStore());

    const auto valueAt = [input, keyColumn, readKeyed, readKey, &properties, &mapBuffer](size_t row) {
        const std::optional<types::String::Primitive> key = readKey(keyColumn, row);

        MapEntryView entry;
        if (!key || !findKeyedEntry(readKeyed(input, row), *key, properties, mapBuffer, entry)) {
            return CellView::nullElement();
        }

        return entry.getValue();
    };

    Column* result = dynamicMapKey->getResult();

    if (result->getKind() == ColumnConst<CellView>::staticKind()) {
        static_cast<ColumnConst<CellView>*>(result)->set(valueAt(0));
        return;
    }

    const size_t rowCount = pairedRowCount(input, keyColumn);

    std::vector<CellView>& outputRaw = static_cast<ColumnVector<CellView>*>(result)->getRaw();

    outputRaw.resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        outputRaw[row] = valueAt(row);
    }
}

template <typename Element>
void NLExecutor::runContainerIndex(NLExecutionContext* context, NLFunctionData* data) {
    const NLContainerIndexData* containerIndex = static_cast<NLContainerIndexData*>(data);

    const Column* base = containerIndex->getBase();
    const Column* index = containerIndex->getIndex();

    const NLContainerReadFunction readBase = containerIndex->getBaseRead();
    const NLSubscriptReadFunction readIndex = containerIndex->getIndexRead();

    NLStepBuffers* const stepBuffers = containerIndex->getStepBuffers();
    stepBuffers->clear();

    MapBuffer<>& mapBuffer = stepBuffers->mapBuffer();
    NLEntityProperties properties(context, &stepBuffers->valueStore());

    const size_t rowCount = pairedRowCount(base, index);

    std::vector<Element>& outputRaw = static_cast<ColumnVector<Element>*>(containerIndex->getResult())->getRaw();

    outputRaw.resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        const CellView element = readContainerElement(readBase(base, row),
                                                      readIndex(index, row),
                                                      properties,
                                                      mapBuffer);

        outputRaw[row] = containerIndexResult<Element>(element);
    }
}

NLHandlerFunction NLExecutor::selectCellContainerIndex() {
    return &runContainerIndex<CellView>;
}

NLHandlerFunction NLExecutor::selectEntityContainerIndex(NLChunkKind kind) {
    if (kind == NLChunkKind::NodeID) {
        return &runContainerIndex<NodeID>;
    } else if (kind == NLChunkKind::EdgeID) {
        return &runContainerIndex<EdgeID>;
    }

    throw IRException("a container index reads its entities out of a list of nodes or of edges");
}

NLHandlerFunction NLExecutor::selectValueContainerIndex(NLChunkKind kind) {
    NLHandlerFunction selected = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { selected = &runContainerIndex<std::optional<Element>>; });

    return selected;
}

NLContainerReadFunction NLExecutor::selectContainerRead(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnVector<ListView>::staticKind()) {
        return &listContainer<ColumnVector<ListView>>;
    } else if (kind == ColumnOptVector<ListView>::staticKind()) {
        return &listContainer<ColumnOptVector<ListView>>;
    } else if (kind == ColumnConst<ListView>::staticKind()) {
        return &listContainer<ColumnConst<ListView>>;
    } else if (kind == ColumnConst<std::optional<ListView>>::staticKind()) {
        return &listContainer<ColumnConst<std::optional<ListView>>>;
    } else if (kind == ColumnVector<MapView>::staticKind()) {
        return &mapContainer<ColumnVector<MapView>>;
    } else if (kind == ColumnOptVector<MapView>::staticKind()) {
        return &mapContainer<ColumnOptVector<MapView>>;
    } else if (kind == ColumnConst<MapView>::staticKind()) {
        return &mapContainer<ColumnConst<MapView>>;
    } else if (kind == ColumnConst<std::optional<MapView>>::staticKind()) {
        return &mapContainer<ColumnConst<std::optional<MapView>>>;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &cellContainer<ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &cellContainer<ColumnConst<CellView>>;
    }

    throw IRException("a container index reads a list column, a map column or a column of tagged cells");
}

NLSubscriptReadFunction NLExecutor::selectSubscriptRead(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnVector<CellView>::staticKind()) {
        return &readSubscript<ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &readSubscript<ColumnConst<CellView>>;
    }

    throw IRException("a container index reads its index out of a column of tagged cells");
}

NLListReadFunction NLExecutor::selectListRead(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnVector<ListView>::staticKind()) {
        return &plainListRead;
    } else if (kind == ColumnOptVector<ListView>::staticKind()) {
        return &optListRead;
    } else if (kind == ColumnConst<ListView>::staticKind()) {
        return &constListRead;
    } else if (kind == ColumnConst<std::optional<ListView>>::staticKind()) {
        return &optConstListRead;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &taggedListRead;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &constTaggedListRead;
    }

    throw IRException("a list slice reads a list column");
}

NLRangeBoundReadFunction NLExecutor::selectRangeBoundRead(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &rangeBound<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &rangeBound<types::UInt64::Primitive>;
        break;

        default:
            throw IRException("nl.range reads its bounds out of integer columns");
        break;
    }
}

NLRangeBoundReadFunction NLExecutor::selectCellBoundRead(const Column* input) {
    const NLRangeBoundReadFunction read = selectCellRowRead<std::optional<types::Int64::Primitive>, &cellBound>(input);
    if (!read) {
        throw IRException("a bound held in type-erased cells is read out of a column of them");
    }

    return read;
}

NLPercentileReadFunction NLExecutor::selectPercentileRead(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &percentileArgument<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &percentileArgument<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &percentileArgument<types::Double::Primitive>;
        break;

        default:
            throw IRException("a percentile is read out of a number column");
        break;
    }
}

NLPercentileReadFunction NLExecutor::selectCellPercentileRead(const Column* input) {
    const NLPercentileReadFunction read = selectCellRowRead<std::optional<types::Double::Primitive>, &cellPercentile>(input);
    if (!read) {
        throw IRException("a percentile held in type-erased cells is read out of a column of them");
    }

    return read;
}

NLListItemReadFunction NLExecutor::selectNodeListItemRead() {
    return &validIDItem<ListBuffer<>::ListItemVariant, NodeID>;
}

NLListItemReadFunction NLExecutor::selectEdgeListItemRead() {
    return &validIDItem<ListBuffer<>::ListItemVariant, EdgeID>;
}

NLPathReadFunction NLExecutor::selectPathRead(const Column* paths) {
    const ColumnKind::Code kind = paths->getKind();

    if (kind == ColumnVector<GraphPath>::staticKind()) {
        return &columnPath;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &taggedColumnPath;
    }

    throw IRException("nodes(), relationships() and length() read a path column, or a column of tagged cells");
}

NLListItemReadFunction NLExecutor::selectPathListItemRead() {
    return &pathItem<ListBuffer<>::ListItemVariant>;
}

NLListItemReadFunction NLExecutor::selectNestedListItemRead() {
    return &plainItem<ListBuffer<>::ListItemVariant, ListView>;
}

NLListItemReadFunction NLExecutor::selectNestedMapListItemRead() {
    return &plainItem<ListBuffer<>::ListItemVariant, MapView>;
}

NLListItemReadFunction NLExecutor::selectTaggedListItemRead() {
    return &taggedColumnItem<ListBuffer<>::ListItemVariant>;
}

NLMapValueReadFunction NLExecutor::selectValueMapValueRead(NLChunkKind kind) {
    NLMapValueReadFunction selected = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { selected = &valueItem<MapBuffer<>::MapItemVariant, Element>; });

    return selected;
}

NLMapValueReadFunction NLExecutor::selectNodeMapValueRead() {
    return &validIDItem<MapBuffer<>::MapItemVariant, NodeID>;
}

NLMapValueReadFunction NLExecutor::selectEdgeMapValueRead() {
    return &validIDItem<MapBuffer<>::MapItemVariant, EdgeID>;
}

NLMapValueReadFunction NLExecutor::selectPathMapValueRead() {
    return &pathItem<MapBuffer<>::MapItemVariant>;
}

NLMapValueReadFunction NLExecutor::selectNestedListMapValueRead() {
    return &plainItem<MapBuffer<>::MapItemVariant, ListView>;
}

NLMapValueReadFunction NLExecutor::selectOptNestedListMapValueRead() {
    return &optItem<MapBuffer<>::MapItemVariant, ListView>;
}

NLMapValueReadFunction NLExecutor::selectNestedMapValueRead() {
    return &plainItem<MapBuffer<>::MapItemVariant, MapView>;
}

NLMapValueReadFunction NLExecutor::selectTaggedMapValueRead() {
    return &taggedColumnItem<MapBuffer<>::MapItemVariant>;
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

NLCaseResetFn NLExecutor::selectCaseReset(NLChunkKind kind) {
    NLCaseResetFn reset = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { reset = &caseResetColumn<Element>; });

    return reset;
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

template <bool TESTS_NULL>
NLUnaryFn NLExecutor::selectNullTest(const Column* operand, LocalMemory* memory, Column*& result) {
    NullTestSelector<TESTS_NULL> selector {._memory = memory};
    ColumnSingleDispatcher<InternalKind::Types::Tuple, NullTestSelector<TESTS_NULL>>::dispatch(operand, selector);

    result = selector._result;
    return selector._fn;
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

NLCaseResetFn NLExecutor::selectOwnNullCaseReset(NLChunkKind kind) {
    if (kind == NLChunkKind::NodeID) {
        return &caseResetOwnNullColumn<NodeID>;
    } else if (kind == NLChunkKind::EdgeID) {
        return &caseResetOwnNullColumn<EdgeID>;
    } else if (kind == NLChunkKind::Path) {
        return &caseResetOwnNullColumn<GraphPath>;
    }

    throw IRException("Only a node, an edge or a path column holds its own null");
}

NLCaseWriteFn NLExecutor::selectOwnNullCaseWrite(NLChunkKind kind, bool untypedNull) {
    if (kind == NLChunkKind::NodeID) {
        return untypedNull ? &caseWriteOwnNull<NodeID> : &caseWriteOwnNullCell<NodeID>;
    } else if (kind == NLChunkKind::EdgeID) {
        return untypedNull ? &caseWriteOwnNull<EdgeID> : &caseWriteOwnNullCell<EdgeID>;
    } else if (kind == NLChunkKind::Path) {
        return untypedNull ? &caseWriteOwnNull<GraphPath> : &caseWriteOwnNullCell<GraphPath>;
    }

    throw IRException("Only a node, an edge or a path column holds its own null");
}

NLFillNullRowsFn NLExecutor::selectFillNullRows(NLChunkKind kind) {
    NLFillNullRowsFn selected = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { selected = &fillNullOptRows<Element>; });

    return selected;
}

NLFillNullRowsFn NLExecutor::selectOwnNullFillNullRows(NLChunkKind kind) {
    if (kind == NLChunkKind::NodeID) {
        return &fillNullRows<NodeID>;
    } else if (kind == NLChunkKind::EdgeID) {
        return &fillNullRows<EdgeID>;
    } else if (kind == NLChunkKind::Path) {
        return &fillNullRows<GraphPath>;
    }

    throw IRException("Only a node, an edge or a path column holds its own null");
}

NLFillNullRowsFn NLExecutor::selectTaggedCellFillNullRows() {
    return &fillNullRows<CellView>;
}

NLCaseWriteFn NLExecutor::selectCaseWrite(NLChunkKind kind,
                                          const Column* value,
                                          bool nullable,
                                          bool untypedNull) {
    NLCaseWriteFn write = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() {
        if (untypedNull) {
            write = &caseWriteNull<Element>;
        } else if (nullable) {
            write = &caseWriteOptCell<Element>;
        } else {
            write = &caseWritePlainCell<Element>;
        }
    });

    return write;
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

NLUnaryFn NLExecutor::selectEntityToNullable(NLChunkKind kind, LocalMemory* memory, Column*& result) {
    const bool readsIDs = kind == NLChunkKind::NodeID || kind == NLChunkKind::EdgeID;
    if (!readsIDs) {
        throw IRException("Only a node or an edge column can be read as a nullable ID column");
    }

    result = memory->alloc<ColumnOptVector<types::UInt64::Primitive>>();

    if (kind == NLChunkKind::NodeID) {
        return &entityToNullableColumn<NodeID>;
    }

    return &entityToNullableColumn<EdgeID>;
}

NLUnaryFn NLExecutor::selectCheckedCast(NLChunkKind kind, const Column* operand, LocalMemory* memory, Column*& result) {
    if (kind == NLChunkKind::NodeID) {
        return selectCheckedCastOf<NodeID>(operand, memory, result);
    } else if (kind == NLChunkKind::EdgeID) {
        return selectCheckedCastOf<EdgeID>(operand, memory, result);
    }

    throw IRException("A tagged cell is checked to hold a node or an edge only");
}

NLUnaryFn NLExecutor::selectToNullable(NLChunkKind kind, const Column* operand, LocalMemory* memory, Column*& result) {
    NLUnaryFn toNullable = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() {
        toNullable = selectToNullableOf<Element>(operand, memory, result);
    });

    return toNullable;
}

template <ColumnOperator Op>
NLBinaryFn NLExecutor::selectBinary(const Column* lhs,
                                    const Column* rhs,
                                    LocalMemory* memory,
                                    Column*& result) {
    using Pairs = PairRestrictions<Op>;

    BinaryOpSelector<Op> selector {._memory = memory};
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           BinaryOpSelector<Op>,
                           typename Pairs::Excluded>::dispatch(lhs, rhs, selector);

    result = selector._result;
    return selector._fn;
}

NLBinaryFn NLExecutor::selectEntityListIndex(NLChunkKind kind,
                                             const Column* lhs,
                                             const Column* rhs,
                                             LocalMemory* memory,
                                             Column*& result) {
    NLBinaryFn selected = nullptr;

    if (kind == NLChunkKind::NodeID) {
        selected = selectEntityListIndexOf<NodeID>(lhs, rhs, memory, result);
    } else if (kind == NLChunkKind::EdgeID) {
        selected = selectEntityListIndexOf<EdgeID>(lhs, rhs, memory, result);
    } else {
        throw IRException("Only a node or an edge column can hold an entity read out of a list");
    }

    if (!selected) {
        throw IRException("An entity is read out of a list column, not out of a type-erased one");
    }

    return selected;
}

NLBinaryFn NLExecutor::selectValueListIndex(NLChunkKind kind,
                                            const Column* lhs,
                                            const Column* rhs,
                                            LocalMemory* memory,
                                            Column*& result) {
    using Pairs = PairRestrictions<OP_INDEX>;

    NLBinaryFn selected = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() {
        using Selector = ValueListIndexSelector<Element>;

        Selector selector {._memory = memory};
        ColumnDoubleDispatcher<typename Pairs::Allowed,
                               Selector,
                               typename Pairs::Excluded>::dispatch(lhs, rhs, selector);

        result = selector._result;
        selected = selector._fn;
    });

    return selected;
}

void NLExecutor::runUnaryFunction(NLExecutionContext* context, NLFunctionData* data) {
    const NLUnaryFunctionData* funcData = static_cast<NLUnaryFunctionData*>(data);
    NLStepBuffers* const stepBuffers = funcData->getStepBuffers();
    stepBuffers->clear();

    funcData->getKernel()(context, funcData->getResult(), funcData->getInput(), stepBuffers);
}

template <typename Functor>
NLUnaryFunctionKernel NLExecutor::selectFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;
    using JustRes = TypeUtils::unwrap_optional_t<Res>;

    // Early exit noop for NULL literals
    if (dynamic_cast<const ColumnConst<PropertyNull>*>(input)) {
        result = memory->alloc<ColumnConst<PropertyNull>>();
        return &functionNullKernel;
    }

    // A column of type-erased cells is served by the counterpart reading one, which
    // answers over the same rows but reads its argument - and its nulls - out of the tag
    if constexpr (HasTaggedCounterpart<Functor>) {
        if (readsTaggedCells(input)) {
            return selectTaggedCellFunction<typename Functor::TaggedCounterpart>(input, memory, result);
        }
    }

    // A constant that can be null - what a conversion of a constant answers - is the one-row
    // form of a nullable column, and is read the way the column is. A function reading its
    // own nulls answers a plain result, so only the argument needs a nullable kind
    constexpr bool hasNullableConstant = InternalKind::Types::contains<std::optional<Arg>>() && !TypedInternalID<Res>;

    if constexpr (hasNullableConstant) {
        if (isNullableConstantOf<Arg>(input)) {
            if constexpr (ReadsItsNulls<Functor>) {
                result = memory->alloc<ColumnConst<Res>>();
                return &functionOptConstKernel<Functor>;
            } else {
                result = memory->alloc<ColumnConst<std::optional<JustRes>>>();
                return &functionNullableConstKernel<Functor>;
            }
        }
    }

    if (input->getContainerKind() == ContainerKind::code<ColumnConst>()) {
        result = memory->alloc<ColumnConst<Res>>();
        return &functionConstKernel<Functor>;
    }

    // An entity answers with the invalid ID where it is absent, so a function answering one
    // needs no nullable column to carry that: it fills the plain ID column every other
    // entity column in the engine is, and its rows say for themselves which matched nothing
    if constexpr (TypedInternalID<Res>) {
        result = memory->alloc<ColumnVector<Res>>();
        return &functionEntityToEntityKernel<Functor>;
    } else {
        constexpr bool hasNullableColumn = InternalKind::Types::contains<std::optional<Arg>>();

        if constexpr (hasNullableColumn) {
            if (inputNullable) {
                if constexpr (ReadsItsNulls<Functor>) {
                    result = memory->alloc<ColumnVector<Res>>();
                    return &functionNullReadingKernel<Functor>;
                } else {
                    result = memory->alloc<ColumnOptVector<JustRes>>();
                    return &functionOptKernel<Functor>;
                }
            }
        }

        // An entity column carries its null in the ID rather than in an optional, so it is
        // read row by row for validity and its result is nullable all the same
        if constexpr (TypedInternalID<typename Functor::ArgType>) {
            result = memory->alloc<ColumnOptVector<JustRes>>();
            return &functionEntityKernel<Functor>;
        }

        result = memory->alloc<ColumnVector<Res>>();
        return &functionVectorKernel<Functor>;
    }
}

template <typename Conversion>
NLUnaryFunctionKernel NLExecutor::selectConversion(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    NLUnaryFunctionKernel kernel = nullptr;

    const auto selectForElement = [&]<typename Element>() {
        using Functor = ConversionFunctorFor<Conversion, Element>::Type;

        if constexpr (!std::is_same_v<Functor, Conversion>) {
            if (!kernel && columnHoldsElement<Element>(input)) {
                kernel = selectFunction<Functor>(input, inputNullable, memory, result);
            }
        }
    };

    selectForElement.template operator()<types::Int64::Primitive>();
    selectForElement.template operator()<types::UInt64::Primitive>();
    selectForElement.template operator()<types::Double::Primitive>();
    selectForElement.template operator()<types::Bool::Primitive>();
    selectForElement.template operator()<types::DateTime::Primitive>();
    selectForElement.template operator()<types::Duration::Primitive>();
    selectForElement.template operator()<MapView>();
    selectForElement.template operator()<types::Point::Primitive>();

    if (kernel) {
        return kernel;
    }

    return selectFunction<Conversion>(input, inputNullable, memory, result);
}

NLUnaryFunctionKernel NLExecutor::selectSize(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::String::Primitive>(input)) {
        return selectFunction<StringSizeFunction>(input, inputNullable, memory, result);
    }

    return selectFunction<ListSizeFunction>(input, inputNullable, memory, result);
}

NLUnaryFunctionKernel NLExecutor::selectIsEmpty(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::String::Primitive>(input)) {
        return selectFunction<StringIsEmptyFunction>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<MapView>(input)) {
        return selectFunction<MapIsEmptyFunction>(input, inputNullable, memory, result);
    }

    return selectFunction<ListIsEmptyFunction>(input, inputNullable, memory, result);
}

NLUnaryFunctionKernel NLExecutor::selectId(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<NodeID>(input)) {
        return selectFunction<IdFunction<NodeID>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<EdgeID>(input)) {
        return selectFunction<IdFunction<EdgeID>>(input, inputNullable, memory, result);
    }

    return selectFunction<TaggedIdFunction>(input, inputNullable, memory, result);
}

NLUnaryFunctionKernel NLExecutor::selectReverse(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::String::Primitive>(input)) {
        return selectFunction<ReverseFunction>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<TaggedReverseFunction>(input, memory, result);
    }

    return selectFunction<ListReverseFunction>(input, inputNullable, memory, result);
}

NLUnaryFunctionKernel NLExecutor::selectKeys(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<NodeID>(input)) {
        return selectFunction<NLKeysFunction<NodeID>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<EdgeID>(input)) {
        return selectFunction<NLKeysFunction<EdgeID>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<NLTaggedKeysFunction>(input, memory, result);
    } else if (columnHoldsElement<MapView>(input)) {
        return selectFunction<MapKeysFunction>(input, inputNullable, memory, result);
    }

    throw IRException("keys() reads a node, a relationship or a map column");
}

NLUnaryFunctionKernel NLExecutor::selectLabels(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<NodeID>(input)) {
        return selectFunction<LabelsFunction>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<NLTaggedLabelsFunction>(input, memory, result);
    }

    throw IRException("labels() reads a node column");
}

NLUnaryFunctionKernel NLExecutor::selectEdgeType(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<EdgeID>(input)) {
        return selectFunction<EdgeTypesFunction>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<NLTaggedEdgeTypeFunction>(input, memory, result);
    }

    throw IRException("type() reads a relationship column");
}

NLUnaryFunctionKernel NLExecutor::selectProperties(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<NodeID>(input)) {
        return selectFunction<NLPropertiesFunction<NodeID>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<EdgeID>(input)) {
        return selectFunction<NLPropertiesFunction<EdgeID>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<NLTaggedPropertiesFunction>(input, memory, result);
    } else if (columnHoldsElement<MapView>(input)) {
        return selectFunction<MapPropertiesFunction>(input, inputNullable, memory, result);
    }

    throw IRException("Reading every property needs a node, a relationship or a map column");
}

template <template <typename> typename NumberFunctor>
NLUnaryFunctionKernel NLExecutor::selectNumberFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::Int64::Primitive>(input)) {
        return selectFunction<NumberFunctor<types::Int64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::UInt64::Primitive>(input)) {
        return selectFunction<NumberFunctor<types::UInt64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Double::Primitive>(input)) {
        return selectFunction<NumberFunctor<types::Double::Primitive>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<typename NumberFunctor<types::Int64::Primitive>::TaggedCounterpart>(input, memory, result);
    }

    throw IRException("A numeric function reads an integer or a double column");
}

NLUnaryFunctionKernel NLExecutor::selectNegateFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::Int64::Primitive>(input)) {
        return selectFunction<NegateFunction<types::Int64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::UInt64::Primitive>(input)) {
        return selectFunction<NegateFunction<types::UInt64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Double::Primitive>(input)) {
        return selectFunction<NegateFunction<types::Double::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Duration::Primitive>(input)) {
        return selectFunction<NegateFunction<types::Duration::Primitive>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<TaggedNegateFunction>(input, memory, result);
    }

    throw IRException("A unary minus reads an integer, a double or a duration column");
}

NLUnaryFunctionKernel NLExecutor::selectUnaryPlusFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::Int64::Primitive>(input)) {
        return selectFunction<UnaryPlusFunction<types::Int64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::UInt64::Primitive>(input)) {
        return selectFunction<UnaryPlusFunction<types::UInt64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Double::Primitive>(input)) {
        return selectFunction<UnaryPlusFunction<types::Double::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Duration::Primitive>(input)) {
        return selectFunction<UnaryPlusFunction<types::Duration::Primitive>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<TaggedUnaryPlusFunction>(input, memory, result);
    }

    throw IRException("A unary plus reads an integer, a double or a duration column");
}

template <FloatFunctionKind Kind>
NLUnaryFunctionKernel NLExecutor::selectFloatFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::Int64::Primitive>(input)) {
        return selectFunction<FloatFunction<Kind, types::Int64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::UInt64::Primitive>(input)) {
        return selectFunction<FloatFunction<Kind, types::UInt64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Double::Primitive>(input)) {
        return selectFunction<FloatFunction<Kind, types::Double::Primitive>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<TaggedFloatFunction<Kind>>(input, memory, result);
    }

    throw IRException("A numeric function reads an integer or a double column");
}

template NLUnaryFunctionKernel NLExecutor::selectNumberFunction<AbsFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectNumberFunction<SignFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Ceil>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Floor>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Round>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Sqrt>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Exp>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Log>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Log10>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Sin>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Cos>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Tan>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Cot>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Asin>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Acos>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Atan>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Degrees>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Radians>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Haversin>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

template NLUnaryFunctionKernel NLExecutor::selectFunction<LabelsFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<EdgeTypesFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<StartNodeFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<EndNodeFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Year>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Month>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Day>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Hour>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Minute>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Second>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Millisecond>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Microsecond>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Years>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Quarters>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Months>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Weeks>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Days>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Hours>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Minutes>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Seconds>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Milliseconds>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Microseconds>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::QuartersOfYear>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MonthsOfYear>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MonthsOfQuarter>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::DaysOfWeek>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MinutesOfHour>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::SecondsOfMinute>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MillisecondsOfSecond>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MicrosecondsOfSecond>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::X>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::Y>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::Z>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::Longitude>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::Latitude>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::Height>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::CRS>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::SRID>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ListHeadFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ListLastFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ListTailFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ToIntegerListFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ToFloatListFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ToBooleanListFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ToStringListFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<CharLengthFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ToUpperFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ToLowerFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<TrimFunction<TrimSide::Start>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<TrimFunction<TrimSide::End>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<TrimFunction<TrimSide::Both>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<NormalizeFunction<UnicodeNormalForm::NFC>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<NormalizeFunction<UnicodeNormalForm::NFD>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<NormalizeFunction<UnicodeNormalForm::NFKC>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<NormalizeFunction<UnicodeNormalForm::NFKD>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<IsNormalizedFunction<UnicodeNormalForm::NFC>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<IsNormalizedFunction<UnicodeNormalForm::NFD>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<IsNormalizedFunction<UnicodeNormalForm::NFKC>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<IsNormalizedFunction<UnicodeNormalForm::NFKD>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

template NLUnaryFunctionKernel NLExecutor::selectConversion<ToIntegerFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectConversion<ToFloatFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectConversion<ToStringFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectConversion<ToBoolFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectConversion<ToDateTimeFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectConversion<ToDurationFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectConversion<ToPointFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

template <auto StrictSelector, typename TaggedFunctor>
NLUnaryFunctionKernel NLExecutor::selectOrNull(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<TaggedFunctor>(input, memory, result);
    }

    return StrictSelector(input, inputNullable, memory, result);
}

template NLUnaryFunctionKernel NLExecutor::selectOrNull<&NLExecutor::selectConversion<ToIntegerFunction>, TaggedToIntegerOrNullFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectOrNull<&NLExecutor::selectConversion<ToFloatFunction>, TaggedToFloatOrNullFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectOrNull<&NLExecutor::selectConversion<ToBoolFunction>, TaggedToBooleanOrNullFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectOrNull<&NLExecutor::selectConversion<ToStringFunction>, TaggedToStringOrNullFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

void NLExecutor::runSortReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLSortResetData* reset = static_cast<NLSortResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runSortCollect(NLExecutionContext* context, NLFunctionData* data) {
    const NLSortCollectData* collect = static_cast<NLSortCollectData*>(data);

    // Append this step's chunk of every column onto its buffer's tail; the
    // columns are taken together so the buffers stay row-aligned.
    for (const NLSortCollectData::Append& append : collect->appends()) {
        appendCollectedChunk(append, collect->getState()->values());
    }

    // For a bounded (top-K) accumulator, drop all but the best k once the buffers
    // have grown past the bound, so memory stays O(k) rather than O(rows). A
    // no-op for an unbounded sort.
    collect->getState()->trimIfNeeded();
}

void NLExecutor::runSortLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLSortLoopData* loopData = static_cast<NLSortLoopData*>(data);
    NLSortState* state = loopData->getState();

    // Sort the accumulated rows once: the permutation is the global row order the
    // emit chunks read in.
    state->sort();
    const std::vector<size_t>& permutation = state->permutation().getRaw();
    const size_t totalRows = permutation.size();

    const NLStmtContainer* loopBody = loopData->getStmts();
    const size_t chunkSize = context->getChunkSize();
    ColumnVector<size_t>* indices = loopData->getIndices();

    const NLLimitState* limit = loopData->getLimit();

    // Re-chunk the sorted rows: each step gathers the next chunkSize rows, in
    // permutation order, into the loop variables, then runs the body (nl.output).
    // The last chunk may be partial; an empty result runs the body zero times.
    const auto runIteration = [&](size_t offset) {
        const size_t stepRows = std::min(chunkSize, totalRows - offset);

        std::vector<size_t>& indicesRaw = indices->getRaw();
        indicesRaw.assign(permutation.begin() + offset, permutation.begin() + offset + stepRows);

        for (const NLCarriedColumn& column : loopData->columns()) {
            const NLGatherFunction gather = column.getGatherFunc();
            gather(column.getInput(), indices, column.getOutput());
        }

        runBody(context, loopBody);
    };

    if (limit) {
        for (size_t offset = 0; offset < totalRows && limit->getRemaining() > 0; offset += chunkSize) {
            runIteration(offset);
        }
    } else {
        for (size_t offset = 0; offset < totalRows; offset += chunkSize) {
            runIteration(offset);
        }
    }
}

void NLExecutor::runRowReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLRowResetData* reset = static_cast<NLRowResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runRowCollect(NLExecutionContext* context, NLFunctionData* data) {
    const NLRowCollectData* collect = static_cast<NLRowCollectData*>(data);

    for (const NLSortCollectData::Append& append : collect->appends()) {
        appendCollectedChunk(append, collect->getState()->values());
    }
}

void NLExecutor::runRowLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLRowLoopData* loopData = static_cast<NLRowLoopData*>(data);
    const size_t totalRows = loopData->getState()->getRowCount();

    const NLStmtContainer* loopBody = loopData->getStmts();
    const size_t chunkSize = context->getChunkSize();
    ColumnVector<size_t>* indices = loopData->getIndices();

    const NLLimitState* limit = loopData->getLimit();

    const auto runIteration = [&](size_t offset) {
        const size_t stepRows = std::min(chunkSize, totalRows - offset);

        std::vector<size_t>& indicesRaw = indices->getRaw();
        indicesRaw.resize(stepRows);
        std::iota(indicesRaw.begin(), indicesRaw.end(), offset);

        for (const NLCarriedColumn& column : loopData->columns()) {
            const NLGatherFunction gather = column.getGatherFunc();
            gather(column.getInput(), indices, column.getOutput());
        }

        runBody(context, loopBody);
    };

    if (limit) {
        for (size_t offset = 0; offset < totalRows && limit->getRemaining() > 0; offset += chunkSize) {
            runIteration(offset);
        }
    } else {
        for (size_t offset = 0; offset < totalRows; offset += chunkSize) {
            runIteration(offset);
        }
    }
}

void NLExecutor::runOptionalReset(NLExecutionContext* context, NLFunctionData* data) {
    NLOptionalResetData* reset = static_cast<NLOptionalResetData*>(data);
    NLOptionalState* state = reset->getState();

    state->reset();

    // The tag holds each input row's position, so the pattern carries it the way it
    // carries any other column and the collect reads the rows it matched off it.
    std::vector<uint64_t>& tagRaw = reset->getTag()->getRaw();
    tagRaw.resize(state->getRowCount());
    std::iota(tagRaw.begin(), tagRaw.end(), uint64_t {0});
}

void NLExecutor::runOptionalCollect(NLExecutionContext* context, NLFunctionData* data) {
    NLOptionalCollectData* collect = static_cast<NLOptionalCollectData*>(data);
    NLOptionalState* state = collect->getState();

    for (const NLSortCollectData::Append& append : collect->appends()) {
        appendCollectedChunk(append, collect->getState()->values());
    }

    const ColumnVector<uint64_t>* tag = collect->getTag();
    if (!tag) {
        // No input row to tag: the step is the single empty row, which this step's rows
        // match - and a predicate that cut them all leaves nothing to match it.
        const std::vector<NLSortCollectData::Append>& appends = collect->appends();
        bioassert(!appends.empty(), "nl.optional_collect needs at least one column");

        if (appends.front()._input->size() > 0) {
            state->markMatched(0);
        }

        return;
    }

    for (const uint64_t row : tag->getRaw()) {
        state->markMatched(row);
    }
}

void NLExecutor::runOptionalDrainLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLOptionalDrainLoopData* loopData = static_cast<NLOptionalDrainLoopData*>(data);
    NLOptionalState* state = loopData->getState();

    const NLStmtContainer* loopBody = loopData->getStmts();
    const size_t chunkSize = context->getChunkSize();
    ColumnVector<size_t>* indices = loopData->getIndices();
    std::vector<size_t>& indicesRaw = indices->getRaw();

    const NLLimitState* limit = loopData->getLimit();
    const auto budgetLeft = [&]() { return !limit || limit->getRemaining() > 0; };

    // The rows the pattern matched, re-chunked out of the buffers the collect grew. A
    // chunk never mixes them with the padded ones: the two are gathered from different
    // columns, so each step reads one or the other.
    const size_t matchedRows = state->getMatchedRowCount();
    for (size_t offset = 0; offset < matchedRows && budgetLeft(); offset += chunkSize) {
        const size_t stepRows = std::min(chunkSize, matchedRows - offset);

        indicesRaw.resize(stepRows);
        std::iota(indicesRaw.begin(), indicesRaw.end(), offset);

        for (const NLOptionalDrainLoopData::DrainColumn& column : loopData->columns()) {
            column._gather(column._buffer, indices, column._output);
        }

        runBody(context, loopBody);
    }

    // Then one row per input row nothing matched: its own values in the columns the
    // pattern joined onto, and null in the ones the pattern binds.
    const std::vector<size_t>& missedRaw = state->missedRows().getRaw();
    const size_t missedRows = missedRaw.size();
    for (size_t offset = 0; offset < missedRows && budgetLeft(); offset += chunkSize) {
        const size_t stepRows = std::min(chunkSize, missedRows - offset);

        indicesRaw.assign(missedRaw.begin() + offset, missedRaw.begin() + offset + stepRows);

        for (const NLOptionalDrainLoopData::DrainColumn& column : loopData->columns()) {
            if (column._input) {
                column._gather(column._input, indices, column._output);
            } else {
                column._fillNull(column._output, stepRows);
            }
        }

        runBody(context, loopBody);
    }
}

void NLExecutor::runSubqueryExpressionReset(NLExecutionContext* context, NLFunctionData* data) {
    NLSubqueryExpressionResetData* reset = static_cast<NLSubqueryExpressionResetData*>(data);
    NLSubqueryExpressionState* state = reset->getState();

    state->reset();

    // The tag holds each input row's position, so the body carries it the way it carries
    // any other column and the mark reads the rows it reached off it.
    std::vector<uint64_t>& tagRaw = reset->getTag()->getRaw();
    tagRaw.resize(state->getRowCount());
    std::iota(tagRaw.begin(), tagRaw.end(), uint64_t {0});
}

void NLExecutor::runExistsMark(NLExecutionContext* context, NLFunctionData* data) {
    NLExistsMarkData* mark = static_cast<NLExistsMarkData*>(data);
    NLExistsState* state = mark->getState();

    const ColumnVector<uint64_t>* tag = mark->getTag();
    if (!tag) {
        // Every row of this step answers for one input row, which they mark - and a
        // predicate that cut them all leaves nothing to mark it.
        const std::vector<const Column*>& columns = mark->columns();
        bioassert(!columns.empty(), "nl.exists_mark needs at least one column when it has no tag");

        if (columns.front()->size() > 0) {
            state->markMatched(untaggedRow(mark->getRow()));
        }

        return;
    }

    for (const uint64_t row : tag->getRaw()) {
        state->markMatched(row);
    }
}

void NLExecutor::runExistsResult(NLExecutionContext* context, NLFunctionData* data) {
    NLExistsResultData* result = static_cast<NLExistsResultData*>(data);
    const NLExistsState* state = result->getState();

    const std::vector<bool>& matched = state->matched();

    std::vector<CustomBool>& answer = result->getResult()->getRaw();
    answer.resize(matched.size());

    std::copy(matched.begin(), matched.end(), answer.begin());
}

void NLExecutor::runCountSubqueryTally(NLExecutionContext* context, NLFunctionData* data) {
    NLCountSubqueryTallyData* tally = static_cast<NLCountSubqueryTallyData*>(data);
    NLCountSubqueryState* state = tally->getState();

    const ColumnVector<uint64_t>* tag = tally->getTag();
    if (!tag) {
        const Column* rows = tally->getRows();
        state->addRows(untaggedRow(tally->getRow()), rows->size());

        return;
    }

    state->addTaggedRows(tag->getRaw());
}

void NLExecutor::runCountSubqueryResult(NLExecutionContext* context, NLFunctionData* data) {
    NLCountSubqueryResultData* result = static_cast<NLCountSubqueryResultData*>(data);
    const NLCountSubqueryState* state = result->getState();

    const std::vector<uint64_t>& counts = state->getCounts();

    std::vector<uint64_t>& answer = result->getResult()->getRaw();
    answer.resize(counts.size());

    std::copy(counts.begin(), counts.end(), answer.begin());
}

void NLExecutor::runDistinctReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLDistinctResetData* reset = static_cast<NLDistinctResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runDistinctFilter(NLExecutionContext* context, NLFunctionData* data) {
    NLDistinctFilterData* filter = static_cast<NLDistinctFilterData*>(data);
    NLDistinctState* state = filter->getState();

    const std::vector<NLDistinctFilterData::FilterColumn>& columns = filter->columns();
    bioassert(!columns.empty(), "nl.distinct_filter needs at least one column");

    // Every column is row-aligned, so the first sizes this step's row set.
    const size_t rowCount = columns.front()._input->size();

    // A row survives iff its key - every column's value at that row - is new to the hash
    // table, which is when the row creates a group in it. A duplicate later in the same
    // chunk finds the group an earlier row of it created.
    NLGroupHashTable& hashTable = state->hashTable();
    hashTable.assign(filter->getKeyColumns(), rowCount);

    const std::vector<size_t>& newGroupRows = hashTable.getNewGroupRows();

    ColumnVector<size_t>* indices = filter->getIndices();
    std::vector<size_t>& survivingRaw = indices->getRaw();
    survivingRaw.assign(newGroupRows.begin(), newGroupRows.end());

    // Gather the survivors into the fresh output chunks, in first-seen order, so a
    // downstream consumer reads a genuinely deduped chunk.
    for (const NLDistinctFilterData::FilterColumn& column : columns) {
        column._gather(column._input, indices, column._output);
    }
}

void NLExecutor::runHashJoinReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLHashJoinResetData* reset = static_cast<NLHashJoinResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runHashJoinCollect(NLExecutionContext* context, NLFunctionData* data) {
    NLHashJoinCollectData* collect = static_cast<NLHashJoinCollectData*>(data);
    NLHashJoinState* state = collect->getState();
    NLHashJoinIndex& index = state->getIndex();

    const Column* key = collect->getKeyColumn();
    const size_t rowCount = key->size();
    const size_t firstRow = index.getRowCount();

    const NLKeyHashFunction hashKeys = collect->getKeyHash();
    std::vector<uint64_t>& hashes = *collect->getHashScratch();
    hashKeys(key, hashes);

    // Appending before indexing puts every row a key could join with in the buffer, so a
    // new row's key is compared against one already indexed through that buffer alone.
    for (const NLSortCollectData::Append& append : collect->appends()) {
        appendCollectedChunk(append, state->values());
    }

    index.addRows(rowCount);

    const Column* buildKey = collect->getBuildKeyColumn();
    const NLKeyEqualFunction keyEqual = collect->getKeyEqual();
    const NLKeyIsMatchableFunction keyIsMatchable = collect->getKeyIsMatchable();

    // A key no probe key can match - a null, a NaN - is indexed under nothing, so no probe
    // row reaches it.
    for (size_t row = 0; row < rowCount; row++) {
        if (!keyIsMatchable(key, row)) {
            continue;
        }

        const uint64_t hash = hashes[row];
        const size_t bufferRow = firstRow + row;
        const size_t group = findKeyGroup(index, hash, buildKey, bufferRow, buildKey, keyEqual);

        if (group == NLHashJoinIndex::noRow) {
            index.openGroup(bufferRow, hash);
        } else {
            index.addToGroup(group, bufferRow);
        }
    }
}

void NLExecutor::runHashJoinProbeLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLHashJoinProbeLoopData* probe = static_cast<NLHashJoinProbeLoopData*>(data);
    const NLHashJoinState* state = probe->getState();
    const NLHashJoinIndex& index = state->getIndex();

    const Column* key = probe->getKeyColumn();
    const Column* buildKey = probe->getBuildKeyColumn();
    const size_t rowCount = key->size();

    const NLKeyHashFunction hashKeys = probe->getKeyHash();
    const NLKeyEqualFunction keyEqual = probe->getKeyEqual();
    const NLKeyIsMatchableFunction keyIsMatchable = probe->getKeyIsMatchable();

    std::vector<uint64_t>& hashes = *probe->getHashScratch();
    hashKeys(key, hashes);

    ColumnVector<size_t>* probeIndices = probe->getProbeIndices();
    ColumnVector<size_t>* buildIndices = probe->getBuildIndices();
    std::vector<size_t>& probeRaw = probeIndices->getRaw();
    std::vector<size_t>& buildRaw = buildIndices->getRaw();

    const size_t chunkSize = context->getChunkSize();
    const NLLimitState* limit = probe->getLimit();
    const NLStmtContainer* loopBody = probe->getStmts();

    // Walk the pairs a chunk at a time, holding the probe row reached and the next row of
    // its group to pair, so a step can start and end partway through one row's matches.
    // noRow with nothing left to pair is what advances the probe row.
    size_t probeRow = 0;
    size_t match = NLHashJoinIndex::noRow;
    bool inGroup = false;

    while (probeRow < rowCount) {
        const size_t remaining = limit ? limit->getRemaining() : chunkSize;
        if (remaining == 0) {
            return;
        }

        const size_t stepBudget = std::min(chunkSize, remaining);

        probeRaw.clear();
        buildRaw.clear();

        while (probeRow < rowCount && probeRaw.size() < stepBudget) {
            if (!inGroup) {
                if (!keyIsMatchable(key, probeRow)) {
                    probeRow++;
                    continue;
                }

                match = findKeyGroup(index, hashes[probeRow], key, probeRow, buildKey, keyEqual);
                inGroup = true;
            }

            // Every row of the group carries the key this probe row matched, so the group
            // is walked without comparing again.
            while (match != NLHashJoinIndex::noRow && probeRaw.size() < stepBudget) {
                probeRaw.push_back(probeRow);
                buildRaw.push_back(match);

                match = index.getNextInGroup(match);
            }

            if (match == NLHashJoinIndex::noRow) {
                probeRow++;
                inGroup = false;
            }
        }

        if (probeRaw.empty()) {
            return;
        }

        for (const NLCarriedColumn& column : probe->probeColumns()) {
            const NLGatherFunction gather = column.getGatherFunc();
            gather(column.getInput(), probeIndices, column.getOutput());
        }

        for (const NLCarriedColumn& column : probe->buildColumns()) {
            const NLGatherFunction gather = column.getGatherFunc();
            gather(column.getInput(), buildIndices, column.getOutput());
        }

        runBody(context, loopBody);
    }
}

void NLExecutor::runFilter(NLExecutionContext* context, NLFunctionData* data) {
    NLFilterData* filter = static_cast<NLFilterData*>(data);

    const std::vector<NLFilterData::FilterColumn>& columns = filter->columns();
    bioassert(!columns.empty(), "nl.filter needs at least one column");

    const Column* mask = filter->getMask();

    // Special case for a ColumnConst<Bool> mask where all rows are either kept or
    // filtered
    if (isMaskConstant(mask)) {
        const auto* constMask = static_cast<const ColumnConst<CustomBool>*>(mask);
        const bool passes = !constMask->empty() && constMask->getRaw()._boolean;

        for (const NLFilterData::FilterColumn& column : columns) {
            if (passes) {
                column._output->assign(column._input);
            } else {
                column._output->clear();
            }
        }
        return;
    }

    // General case: Otherwise apply the mask to each column
    ColumnVector<size_t>* indices = filter->getIndices();
    indices->getRaw().clear();
    filter->getSurvivors()(mask, indices);

    for (const NLFilterData::FilterColumn& column : columns) {
        column._gather(column._input, indices, column._output);
    }
}

void NLExecutor::runFetchNodes(NLExecutionContext* context, NLFunctionData* data) {
    NLFetchNodesData* fetch = static_cast<NLFetchNodesData*>(data);

    ColumnVector<size_t>* indices = fetch->getIndices();
    ColumnNodeIDs* nodes = fetch->getNodes();

    indices->getRaw().clear();
    nodes->getRaw().clear();

    fetch->getFetch()(fetch->getIDs(), context, indices, nodes);

    for (const NLFilterData::FilterColumn& column : fetch->getColumns()) {
        column._gather(column._input, indices, column._output);
    }
}

void NLExecutor::runCountReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLCountResetData* reset = static_cast<NLCountResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runCountUpdate(NLExecutionContext* context, NLFunctionData* data) {
    const NLCountUpdateData* update = static_cast<NLCountUpdateData*>(data);

    // Charge this step's non-null rows: the handle returns all rows for an ID
    // chunk, the present values for a nullable value chunk.
    const NLCountFunction count = update->getCount();
    update->getState()->add(count(update->getRows()));
}

void NLExecutor::runCountResult(NLExecutionContext* context, NLFunctionData* data) {
    const NLCountResultData* result = static_cast<NLCountResultData*>(data);

    // The aggregate collapses every counted row to a single result row: write the
    // final tally into the output chunk as one unsigned i64 (the !nl.chunk<ui64>
    // count column). Runs after the producing loop, so the counter holds the whole
    // dataflow's count; nl.output emits this chunk at function scope.
    const size_t count = result->getState()->getCount();
    ColumnVector<uint64_t>* output = static_cast<ColumnVector<uint64_t>*>(result->getOutput());
    std::vector<uint64_t>& raw = output->getRaw();
    raw.assign(1, static_cast<uint64_t>(count));
}

void NLExecutor::runCountScanRows(NLExecutionContext* context, NLFunctionData* data) {
    const NLCountScanRowsData* scanRows = static_cast<NLCountScanRowsData*>(data);

    ColumnVector<uint64_t>* output = static_cast<ColumnVector<uint64_t>*>(scanRows->getOutput());
    std::vector<uint64_t>& raw = output->getRaw();

    // A conjunction naming a label, or a property, the schema never had matches no node, so
    // the scan it stands for contributes no row and the product is empty.
    if (!scanRows->isMatchable()) {
        raw.assign(1, 0);
        return;
    }

    // Each scan contributes the nodes carrying its labels, and crossed together they make
    // the product of those counts. An empty conjunction filters nothing, so its scan is
    // every node of the graph; a named property narrows the one scan it is read from to the
    // nodes that hold it, which is what count(b.name) tallies over a product.
    const GraphReader reader(*context->getView());
    const std::span<const LabelSet> labelSets = scanRows->getLabelSets();
    const PropertyTypeID propertyTypeID = scanRows->getPropertyTypeID();
    const size_t propertyScan = scanRows->getPropertyScan();

    size_t rows = 1;
    for (size_t scanIndex = 0; scanIndex < labelSets.size(); scanIndex++) {
        const LabelSet& labelset = labelSets[scanIndex];
        const LabelSetHandle labelsetHandle {labelset};

        if (propertyTypeID.isValid() && scanIndex == propertyScan) {
            rows *= reader.getNodeCountWithProperty(labelsetHandle, propertyTypeID);
        } else if (labelset.empty()) {
            rows *= reader.getNodeCount();
        } else {
            rows *= reader.getNodeCountMatchingLabelset(labelsetHandle);
        }
    }

    raw.assign(1, static_cast<uint64_t>(rows));
}

void NLExecutor::runAggregateReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLAggregateResetData* reset = static_cast<NLAggregateResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runAggregateUpdate(NLExecutionContext* context, NLFunctionData* data) {
    const NLAggregateUpdateData* update = static_cast<NLAggregateUpdateData*>(data);
    NLAggregateState* state = update->getState();

    // Fold this step's non-null values into the state the way its kind and value type
    // demand (add for sum/avg, keep the extreme for min/max).
    update->getFold()(state->getPlace(), update->getInput(), state->getValues());
}

void NLExecutor::runAggregateResult(NLExecutionContext* context, NLFunctionData* data) {
    const NLAggregateResultData* result = static_cast<NLAggregateResultData*>(data);

    // The aggregate collapses every folded row to a single result row: write the
    // reduced value into the output chunk's one nullable row. Runs after the
    // producing loop, so the state holds the whole dataflow's reduction;
    // nl.output emits this chunk at function scope.
    NLStepBuffers* const stepBuffers = result->getStepBuffers();
    stepBuffers->clear();

    result->getState()->emit(result->getOutput(), stepBuffers);
}

void NLExecutor::runGroupAggregateReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLGroupAggregateResetData* reset = static_cast<NLGroupAggregateResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runGroupAggregateUpdate(NLExecutionContext* context, NLFunctionData* data) {
    NLGroupAggregateUpdateData* update = static_cast<NLGroupAggregateUpdateData*>(data);
    NLGroupAggregateState* state = update->getState();

    NLGroupTable& groupTable = state->groupTable();
    const std::vector<NLGroupTable::KeyColumn>& keyColumns = groupTable.keyColumns();
    NLGroupAggregates& aggregates = state->aggregates();
    bioassert(!keyColumns.empty(), "nl.group_aggregate_update needs at least one grouping key");

    // Every column is row-aligned, so the first grouping key sizes this step's rows.
    const size_t rowCount = keyColumns.front()._input->size();

    // That alignment is this step's precondition, not a property of the op's types:
    // the group assignment is computed once per key row, then indexed by each fold as
    // it walks its own input, so a column of another length folds rows into the wrong
    // group - or, when it is the longer one, reads past the assignments entirely.
    // Generated IR places the update where every column is bound together, so a
    // mismatch here means malformed IR.
    for (const NLGroupTable::KeyColumn& keyColumn : keyColumns) {
        bioassert(keyColumn._input->size() == rowCount,
                  "nl.group_aggregate_update grouping keys must be row-aligned");
    }

    for (const NLGroupAggregates::Aggregate& aggregate : aggregates.getAggregates()) {
        bioassert(aggregate._input->size() == rowCount,
                  "nl.group_aggregate_update aggregate inputs must be row-aligned with the grouping keys");
    }

    if (rowCount == 0) {
        return;
    }

    groupTable.assign(rowCount);

    aggregates.grow(groupTable.getGroupCount());
    aggregates.fold(groupTable.getRowGroupColumn());
    aggregates.compactValues();
}

void NLExecutor::runGroupAggregateLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLGroupAggregateLoopData* loopData = static_cast<NLGroupAggregateLoopData*>(data);
    NLGroupAggregateState* state = loopData->getState();

    NLGroupTable& groupTable = state->groupTable();
    const size_t totalGroups = groupTable.getGroupCount();
    const size_t chunkSize = context->getChunkSize();
    const NLStmtContainer* loopBody = loopData->getStmts();

    NLGroupAggregates& aggregates = state->aggregates();

    const NLLimitState* limit = loopData->getLimit();
    NLStepBuffers* const stepBuffers = loopData->getStepBuffers();

    // Re-chunk the accumulated groups: each step materializes the next chunk of
    // group rows - the key values sliced from the buffers and each aggregate
    // finalized from the per-group state - into the loop variables, then runs the
    // body (the nl.output) per chunk. An empty result (no group) runs the body zero
    // times, so a grouped aggregate over no row emits nothing.
    const auto runIteration = [&](size_t offset) {
        const size_t stepGroups = std::min(chunkSize, totalGroups - offset);

        groupTable.emitKeys(offset, stepGroups);

        stepBuffers->clear();
        aggregates.emit(offset, stepGroups, stepBuffers);

        runBody(context, loopBody);
    };

    if (limit) {
        for (size_t offset = 0; offset < totalGroups && limit->getRemaining() > 0; offset += chunkSize) {
            runIteration(offset);
        }
    } else {
        for (size_t offset = 0; offset < totalGroups; offset += chunkSize) {
            runIteration(offset);
        }
    }
}

void NLExecutor::runCollectReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLCollectResetData* reset = static_cast<NLCollectResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runCollectUpdate(NLExecutionContext* context, NLFunctionData* data) {
    NLCollectUpdateData* update = static_cast<NLCollectUpdateData*>(data);
    NLCollectState* state = update->getState();

    NLGroupTable& groupTable = state->groupTable();
    const std::vector<NLGroupTable::KeyColumn>& keyColumns = groupTable.keyColumns();
    std::vector<NLCollectState::ValueColumn>& valueColumns = state->valueColumns();

    // Every column is row-aligned. With grouping keys the first key sizes this step's
    // rows; ungrouped (no key), the first collected value column sizes it.
    const size_t rowCount = keyColumns.empty() ? valueColumns.front()._input->size()
                                               : keyColumns.front()._input->size();

    // That alignment is this step's precondition, not a property of the op's types:
    // the group assignment is computed once per key row, then indexed by the fold as it
    // walks the value column, so a column of another length appends values to the wrong
    // group - or, when the value column is the longer one, reads past the assignments
    // entirely. Generated IR places the update where every column is bound together, so
    // a mismatch here means malformed IR.
    for (const NLGroupTable::KeyColumn& keyColumn : keyColumns) {
        bioassert(keyColumn._input->size() == rowCount,
                  "nl.collect_update grouping keys must be row-aligned");
    }

    for (const NLCollectState::ValueColumn& valueColumn : valueColumns) {
        bioassert(valueColumn._input->size() == rowCount,
                  "nl.collect_update value columns must be row-aligned with the grouping keys");
    }

    NLGroupAggregates& aggregates = state->aggregates();
    for (const NLGroupAggregates::Aggregate& aggregate : aggregates.getAggregates()) {
        bioassert(aggregate._input->size() == rowCount,
                  "nl.collect_update aggregate inputs must be row-aligned with the grouping keys");
    }

    if (rowCount == 0) {
        return;
    }

    groupTable.assign(rowCount);

    const size_t groupCount = groupTable.getGroupCount();
    const ColumnVector<size_t>* rowGroups = groupTable.getRowGroupColumn();
    const std::vector<size_t>& groupIndices = rowGroups->getRaw();

    // Grow the per-group position lists to the new group count, then append this step's
    // present values to their groups' lists.
    for (NLCollectState::ValueColumn& valueColumn : valueColumns) {
        valueColumn._groupPositions.resize(groupCount);

        if (valueColumn._distinct) {
            valueColumn._distinct->assign(rowGroups, valueColumn._input);
        }

        valueColumn._fold(valueColumn._buffer,
                          valueColumn._input,
                          groupIndices,
                          valueColumn._groupPositions,
                          valueColumn._distinct,
                          state->collectedValues());
    }

    // The reductions taken over the same groups fold beside the list, off the group
    // assignment computed once above, exactly as nl.group_aggregate_update folds them.
    aggregates.grow(groupCount);
    aggregates.fold(rowGroups);
    aggregates.compactValues();
}

NLCollectFoldFunction NLExecutor::selectCollectFold(NLChunkKind kind) {
    NLCollectFoldFunction selected = nullptr;
    dispatchCollectedChunkKind(kind, [&]<typename Element>() { selected = &collectFold<Element>; });

    return selected;
}

// The collect(DISTINCT x) fold for a column of this kind: collect's own domain, with each of
// a group's values buffered once
NLCollectFoldFunction NLExecutor::selectCollectDistinctFold(NLChunkKind kind) {
    NLCollectFoldFunction selected = nullptr;
    dispatchCollectedChunkKind(kind, [&]<typename Element>() { selected = &collectFoldDistinct<Element>; });

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

NLUnwindCollectValueEmitFunction NLExecutor::selectUnwindCollectValueEmit(NLChunkKind kind) {
    NLUnwindCollectValueEmitFunction selected = nullptr;
    dispatchCollectedChunkKind(kind, [&]<typename Element>() { selected = &unwindCollectValueEmit<Element>; });

    return selected;
}

NLCollectListEmitFunction NLExecutor::selectCollectListEmit(NLChunkKind kind) {
    NLCollectListEmitFunction selected = nullptr;
    dispatchCollectedChunkKind(kind, [&]<typename Element>() { selected = &collectListEmit<Element>; });

    return selected;
}

void NLExecutor::selectCollectEntityHandlers(NLChunkKind kind,
                                             bool distinctValues,
                                             NLCollectFoldFunction& fold,
                                             NLCollectListEmitFunction& listEmit) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return selectCollectIDHandlers<NodeID>(distinctValues, fold, listEmit);
        break;

        case NLChunkKind::EdgeID:
            return selectCollectIDHandlers<EdgeID>(distinctValues, fold, listEmit);
        break;

        default:
            throw IRException("collect does not support this chunk kind");
        break;
    }
}

// A list cell is present in every row, so it folds with no null to drop; only the dedup
// differs, keying on the elements rather than on the cell.
template <typename Container>
void NLExecutor::selectCollectContainerHandlers(bool distinctValues,
                                                NLCollectFoldFunction& fold,
                                                NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectContainerFoldDistinct<Container> : &collectCellFold<Container>;
    listEmit = &collectListEmit<Container>;
}

template <typename Container>
void NLExecutor::selectCollectOptContainerHandlers(bool distinctValues,
                                                   NLCollectFoldFunction& fold,
                                                   NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectOptContainerFoldDistinct<Container> : &collectOptContainerFold<Container>;
    listEmit = &collectListEmit<Container>;
}

template void NLExecutor::selectCollectContainerHandlers<ListView>(bool distinctValues,
                                                                   NLCollectFoldFunction& fold,
                                                                   NLCollectListEmitFunction& listEmit);
template void NLExecutor::selectCollectContainerHandlers<MapView>(bool distinctValues,
                                                                  NLCollectFoldFunction& fold,
                                                                  NLCollectListEmitFunction& listEmit);
template void NLExecutor::selectCollectOptContainerHandlers<ListView>(bool distinctValues,
                                                                      NLCollectFoldFunction& fold,
                                                                      NLCollectListEmitFunction& listEmit);
template void NLExecutor::selectCollectOptContainerHandlers<MapView>(bool distinctValues,
                                                                     NLCollectFoldFunction& fold,
                                                                     NLCollectListEmitFunction& listEmit);

// A type-erased cell carries its own type, so the fold drops the ones tagged null and the
// emit writes each survivor back under the type its tag names.
void NLExecutor::selectCollectTaggedHandlers(bool distinctValues,
                                             NLCollectFoldFunction& fold,
                                             NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectTaggedFoldDistinct
                          : &collectTaggedFold;
    listEmit = &collectTaggedListEmit;
}

void NLExecutor::selectCollectPathHandlers(bool distinctValues,
                                           NLCollectFoldFunction& fold,
                                           NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectPathFoldDistinct : &collectPathFold;
    listEmit = &collectTaggedListEmit;
}

void NLExecutor::runUnwindCollectLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLUnwindCollectLoopData* loopData = static_cast<NLUnwindCollectLoopData*>(data);
    NLCollectState* state = loopData->getState();

    const size_t chunkSize = context->getChunkSize();
    const NLStmtContainer* loopBody = loopData->getStmts();

    NLGroupTable& groupTable = state->groupTable();
    NLCollectState::ValueColumn& unwound = state->unwoundColumn();
    const std::vector<std::vector<size_t>>& groupPositions = unwound._groupPositions;
    const size_t totalGroups = groupPositions.size();

    ColumnVector<size_t>* groupIndices = loopData->getGroupIndices();
    ColumnVector<size_t>* positions = loopData->getPositions();

    // Walk every (group, element) pair in group order. currentGroup / indexInGroup is
    // the cursor into that flattened sequence; empty groups (all values null) are
    // skipped, so they contribute no row - matching UNWIND of an empty list.
    size_t currentGroup = 0;
    size_t indexInGroup = 0;
    while (currentGroup < totalGroups && groupPositions[currentGroup].empty()) {
        currentGroup++;
    }

    while (currentGroup < totalGroups) {
        std::vector<size_t>& groupIndicesRaw = groupIndices->getRaw();
        std::vector<size_t>& positionsRaw = positions->getRaw();
        groupIndicesRaw.clear();
        positionsRaw.clear();

        // Fill up to chunkSize rows, each the next (group, element) pair.
        while (groupIndicesRaw.size() < chunkSize && currentGroup < totalGroups) {
            const std::vector<size_t>& groupPos = groupPositions[currentGroup];

            groupIndicesRaw.push_back(currentGroup);
            positionsRaw.push_back(groupPos[indexInGroup]);
            indexInGroup++;

            if (indexInGroup == groupPos.size()) {
                indexInGroup = 0;
                currentGroup++;
                while (currentGroup < totalGroups && groupPositions[currentGroup].empty()) {
                    currentGroup++;
                }
            }
        }

        // The key values repeat once per element (gather by the per-row group index);
        // the element values come from the flat buffer at this chunk's positions.
        groupTable.gatherKeys(groupIndices);

        unwound._unwindCollectEmit(unwound._buffer, positions, unwound._output);

        runBody(context, loopBody);
    }
}

void NLExecutor::runCollectLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLCollectLoopData* loopData = static_cast<NLCollectLoopData*>(data);
    NLCollectState* state = loopData->getState();

    NLGroupTable& groupTable = state->groupTable();
    const size_t totalGroups = groupTable.getGroupCount();
    const size_t chunkSize = context->getChunkSize();
    const NLStmtContainer* loopBody = loopData->getStmts();

    NLGroupAggregates& aggregates = state->aggregates();
    NLStepBuffers* const stepBuffers = loopData->getStepBuffers();

    // Re-chunk the groups: each step slices the next chunk of key values from the key
    // buffers and materializes one list cell per group (a ListView over that group's
    // run in the list buffer), then runs the body. A grouped collect over no row has no
    // group, so the body runs zero times and it emits nothing; an ungrouped collect
    // always carries the single group the reset created, so it emits exactly one row -
    // holding the empty list when no value was folded.
    for (size_t offset = 0; offset < totalGroups; offset += chunkSize) {
        const size_t stepGroups = std::min(chunkSize, totalGroups - offset);

        groupTable.emitKeys(offset, stepGroups);

        for (NLCollectState::ValueColumn& valueColumn : state->valueColumns()) {
            valueColumn._listEmit(valueColumn._buffer,
                                  valueColumn._groupPositions,
                                  offset,
                                  stepGroups,
                                  state->listBuffer(),
                                  valueColumn._output);
        }

        stepBuffers->clear();
        aggregates.emit(offset, stepGroups, stepBuffers);

        runBody(context, loopBody);
    }
}

void NLExecutor::runShortestPathReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLShortestPathResetData* reset = static_cast<NLShortestPathResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runShortestPathUpdate(NLExecutionContext* context, NLFunctionData* data) {
    NLShortestPathUpdateData* update = static_cast<NLShortestPathUpdateData*>(data);
    NLShortestPathState* state = update->getState();

    const std::vector<NodeID>& nodes = update->getNodes()->getRaw();

    // A target only needs to be a member; a source seeds the frontier at distance zero, so it
    // goes through the typed accumulator.
    if (update->isTarget()) {
        state->targets().insert(nodes.begin(), nodes.end());
    } else {
        state->addSources(nodes);
    }
}

void NLExecutor::runShortestPathLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLShortestPathLoopData* loopData = static_cast<NLShortestPathLoopData*>(data);

    switch (loopData->getState()->getValueType()) {
        case ValueType::Double:
            return shortestPathSearch<types::Double>(context, loopData);
        break;

        case ValueType::Int64:
            return shortestPathSearch<types::Int64>(context, loopData);
        break;

        case ValueType::UInt64:
            return shortestPathSearch<types::UInt64>(context, loopData);
        break;

        default:
            throw IRException("SHORTESTPATH weight must be an Int64, UInt64 or Double property");
        break;
    }
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

NLFillNullFunction NLExecutor::selectFillNullFunction(NLChunkKind kind) {
    NLFillNullFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &fillNullColumn<ElementType>; });

    return selected;
}

NLFillNullFunction NLExecutor::selectOptFillNullFunction(NLChunkKind kind) {
    NLFillNullFunction fill = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { fill = &fillNullOptColumn<Element>; });

    return fill;
}

NLFillNullFunction NLExecutor::selectOptListFillNull() {
    return &fillNullOptColumn<ListView>;
}

NLFillNullFunction NLExecutor::selectCellFillNull() {
    return &resetTaggedCells;
}

NLGatherFunction NLExecutor::selectGatherFunction(NLChunkKind kind) {
    NLGatherFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &gatherColumn<ElementType>; });

    return selected;
}

NLGatherFunction NLExecutor::selectCountGatherFunction() {
    return &gatherColumn<uint64_t>;
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

NLFetchNodesFunction NLExecutor::selectNodeFetchFunction() {
    return &fetchNamedNodes<NodeID>;
}

NLFetchNodesFunction NLExecutor::selectIntegerFetchFunction(NLChunkKind kind, bool nullable) {
    switch (kind) {
        case NLChunkKind::Int64:
            return nullable ? &fetchNamedNodes<std::optional<types::Int64::Primitive>> : &fetchNamedNodes<types::Int64::Primitive>;
        break;

        case NLChunkKind::UInt64:
            return nullable ? &fetchNamedNodes<std::optional<types::UInt64::Primitive>> : &fetchNamedNodes<types::UInt64::Primitive>;
        break;

        default:
            throw IRException("nl.fetch_nodes reads node IDs out of nodes, integers or tagged cells");
        break;
    }
}

NLFetchNodesFunction NLExecutor::selectTaggedFetchFunction() {
    return &fetchNamedNodes<CellView>;
}

NLBroadcastFunction NLExecutor::selectBlockRepeatFunction(NLChunkKind kind) {
    NLBroadcastFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &blockRepeatColumn<ElementType>; });

    return selected;
}

NLBroadcastFunction NLExecutor::selectCountBlockRepeatFunction() {
    return &blockRepeatColumn<uint64_t>;
}

NLBroadcastFunction NLExecutor::selectConstBlockRepeatFunction() {
    return &blockRepeatConstColumn;
}

NLBroadcastFunction NLExecutor::selectTileFunction(NLChunkKind kind) {
    NLBroadcastFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &tileColumn<ElementType>; });

    return selected;
}

NLBroadcastConstantFunction NLExecutor::selectNullConstantBroadcast() {
    return &broadcastNullColumn;
}

NLBroadcastConstantFunction NLExecutor::selectConstantListBroadcast() {
    return &broadcastConstantPlainColumn<ListView>;
}

NLBroadcastConstantFunction NLExecutor::selectConstantMapBroadcast() {
    return &broadcastConstantPlainColumn<MapView>;
}

NLBroadcastConstantFunction NLExecutor::selectConstantCellBroadcast() {
    return &broadcastConstantPlainColumn<CellView>;
}

NLBroadcastConstantFunction NLExecutor::selectConstantPlainBroadcast(NLChunkKind kind, const Column* value) {
    bioassert(value->getContainerKind() == ContainerKind::code<ColumnConst>(),
              "A boolean, a node, an edge, a path or a walk laid out over rows is read as the constant nl.row_constant made of it");

    NLBroadcastConstantFunction fill = nullptr;
    dispatchChunkKind(kind, [&]<typename Element>() { fill = &broadcastConstantPlainColumn<Element>; });

    return fill;
}

NLBroadcastConstantFunction NLExecutor::selectConstantBroadcast(NLChunkKind kind, const Column* value) {
    const bool isConst = value->getContainerKind() == ContainerKind::code<ColumnConst>();

    NLBroadcastConstantFunction fill = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() {
        if (!isConst) {
            fill = &broadcastSingleRowColumn<Element>;
        } else if (value->getInternalKind() == InternalKind::code<std::optional<Element>>()) {
            fill = &broadcastNullableConstantColumn<Element>;
        } else {
            fill = &broadcastConstantColumn<Element>;
        }
    });

    return fill;
}

NLUnaryFn NLExecutor::selectRowConstant(NLChunkKind kind, const Column* row, LocalMemory* memory, Column*& result) {
    NLUnaryFn read = nullptr;
    dispatchChunkKind(kind, [&]<typename Element>() { read = selectRowConstantOf<Element>(row, memory, result); });

    return read;
}

NLUnaryFn NLExecutor::selectOptRowConstant(NLChunkKind kind, const Column* row, LocalMemory* memory, Column*& result) {
    NLUnaryFn read = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() {
        read = selectOptRowConstantOf<Element>(row, memory, result);
    });

    return read;
}

NLUnaryFn NLExecutor::selectCellRowConstant(const Column* row, LocalMemory* memory, Column*& result) {
    return selectRowConstantOf<CellView>(row, memory, result);
}

// A !storage.any chunk is a ColumnVector<CellView> of fixed-width tagged
// scalars, so the same broadcast templates carry the tag along with the value.
NLBroadcastFunction NLExecutor::selectCellBlockRepeatFunction() {
    return &blockRepeatColumn<CellView>;
}

NLBroadcastFunction NLExecutor::selectCellTileFunction() {
    return &tileColumn<CellView>;
}

NLAppendFunction NLExecutor::selectCellAppendFunction() {
    return &appendColumn<CellView>;
}

NLGatherFunction NLExecutor::selectCellGatherFunction() {
    return &gatherColumn<CellView>;
}

NLCompareFunction NLExecutor::selectCellCompareFunction() {
    return &compareCellColumn;
}

NLKeyAppendFunction NLExecutor::selectCellKeyAppendFunction() {
    return &distinctKeyAppendCellColumn;
}

NLCountFunction NLExecutor::selectCellCountFunction() {
    return &countNonNullElementsColumn;
}

NLGroupKeyGatherFunction NLExecutor::selectCellGroupKeyGatherFunction() {
    return &groupGatherAppendColumn<CellView>;
}

NLCopyFunction NLExecutor::selectCellCopyFunction() {
    return &copyRangeColumn<CellView>;
}

NLCompareFunction NLExecutor::selectListCompareFunction() {
    return &compareContainerColumn<ListView>;
}

// The nullable list family, which labels() produces: a node an OPTIONAL MATCH did not
// match holds no list, where a collected list column holds one in every row.
NLBroadcastFunction NLExecutor::selectOptListBlockRepeatFunction() {
    return &blockRepeatOptColumn<ListView>;
}

NLBroadcastFunction NLExecutor::selectOptListTileFunction() {
    return &tileOptColumn<ListView>;
}

NLAppendFunction NLExecutor::selectOptListAppendFunction() {
    return &appendOptColumn<ListView>;
}

NLOwnedAppendFunction NLExecutor::selectOwnedAppendFunction(const Column* buffer) {
    NLOwnedAppendFunction append = nullptr;
    dispatchViewColumn(buffer, Overloaded {
        [&]<typename ElementType>(const ColumnVector<ElementType>* column) { append = &appendOwnedColumn<ElementType>; },
        [&]<typename Element>(const ColumnOptVector<Element>* column) { append = &appendOwnedOptColumn<Element>; },
    });

    return append;
}

NLReownFunction NLExecutor::selectReownFunction(const Column* column) {
    NLReownFunction reown = nullptr;
    dispatchViewColumn(column, Overloaded {
        [&]<typename ElementType>(const ColumnVector<ElementType>* viewColumn) { reown = &reownColumn<ColumnVector<ElementType>>; },
        [&]<typename Element>(const ColumnOptVector<Element>* viewColumn) { reown = &reownColumn<ColumnOptVector<Element>>; },
    });

    return reown;
}

NLReownRowsFunction NLExecutor::selectReownRowsFunction(const Column* column) {
    NLReownRowsFunction reown = nullptr;
    dispatchViewColumn(column, Overloaded {
        [&]<typename ElementType>(const ColumnVector<ElementType>* viewColumn) { reown = &reownColumnRows<ColumnVector<ElementType>>; },
        [&]<typename Element>(const ColumnOptVector<Element>* viewColumn) { reown = &reownColumnRows<ColumnOptVector<Element>>; },
    });

    return reown;
}

NLGatherFunction NLExecutor::selectOptListGatherFunction() {
    return &gatherOptColumn<ListView>;
}

NLCompareFunction NLExecutor::selectOptListCompareFunction() {
    return &compareOptContainerColumn<ListView>;
}

NLKeyAppendFunction NLExecutor::selectOptListKeyAppendFunction() {
    return &distinctKeyAppendOptListColumn;
}

NLCountFunction NLExecutor::selectOptListCountFunction() {
    return &countPresentColumn<ListView>;
}

NLGroupKeyGatherFunction NLExecutor::selectOptListGroupKeyGatherFunction() {
    return &groupGatherAppendOptColumn<ListView>;
}

NLCopyFunction NLExecutor::selectOptListCopyFunction() {
    return &copyRangeOptColumn<ListView>;
}

NLListItemReadFunction NLExecutor::selectOptNestedListItemRead() {
    return &optItem<ListBuffer<>::ListItemVariant, ListView>;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountOptListFold() {
    return &groupFoldCountPresent<ListView>;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountDistinctOptListFold() {
    return &groupFoldCountDistinctPresent<ListView>;
}

NLCopyFunction NLExecutor::selectCopyFunction(NLChunkKind kind) {
    NLCopyFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &copyRangeColumn<ElementType>; });

    return selected;
}

NLCopyFunction NLExecutor::selectCountCopyFunction() {
    return &copyRangeColumn<uint64_t>;
}

NLCopyFunction NLExecutor::selectConstCopyFunction() {
    return &copyRangeConstColumn;
}

NLGatherFunction NLExecutor::selectConstGatherFunction() {
    return &gatherConstColumn;
}

NLGatherFunction NLExecutor::selectOptGatherFunction(NLChunkKind kind) {
    NLGatherFunction gather = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { gather = &gatherOptColumn<Element>; });

    return gather;
}

NLAppendFunction NLExecutor::selectOptAppendFunction(NLChunkKind kind) {
    NLAppendFunction append = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { append = &appendOptColumn<Element>; });

    return append;
}

NLCopyFunction NLExecutor::selectOptCopyFunction(NLChunkKind kind) {
    NLCopyFunction copy = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { copy = &copyRangeOptColumn<Element>; });

    return copy;
}

NLGroupKeyGatherFunction NLExecutor::selectOptGroupKeyGather(NLChunkKind kind) {
    NLGroupKeyGatherFunction gather = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { gather = &groupGatherAppendOptColumn<Element>; });

    return gather;
}

// count(x) reads has_value(), never the value's bytes, so unlike
// selectOptKeyAppendFunction it counts an embedding
NLCountFunction NLExecutor::selectOptCountFunction(NLChunkKind kind) {
    NLCountFunction count = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { count = &countPresentColumn<Element>; });

    return count;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountOptFold(NLChunkKind kind) {
    NLGroupAggregateFoldFunction fold = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { fold = &groupFoldCountPresent<Element>; });

    return fold;
}

NLBroadcastFunction NLExecutor::selectOptBlockRepeatFunction(NLChunkKind kind) {
    NLBroadcastFunction broadcast = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { broadcast = &blockRepeatOptColumn<Element>; });

    return broadcast;
}

NLBroadcastFunction NLExecutor::selectOptTileFunction(NLChunkKind kind) {
    NLBroadcastFunction broadcast = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { broadcast = &tileOptColumn<Element>; });

    return broadcast;
}

NLAppendFunction NLExecutor::selectAppendFunction(NLChunkKind kind) {
    NLAppendFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &appendColumn<ElementType>; });

    return selected;
}

NLAppendFunction NLExecutor::selectCountAppendFunction() {
    return &appendColumn<uint64_t>;
}

NLKeyAppendFunction NLExecutor::selectKeyAppendFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &distinctKeyAppendColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &distinctKeyAppendColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &distinctKeyAppendColumn<EdgeTypeID>;
        break;

        case NLChunkKind::LabelSetID:
            return &distinctKeyAppendColumn<LabelSetID>;
        break;

        case NLChunkKind::LabelID:
            return &distinctKeyAppendColumn<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &distinctKeyAppendColumn<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &distinctKeyAppendPlainColumn<ValueType>;
        break;

        case NLChunkKind::UInt64:
            return &distinctKeyAppendPlainColumn<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Int64:
            return &distinctKeyAppendPlainColumn<types::Int64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &distinctKeyAppendPlainColumn<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &distinctKeyAppendPlainColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &distinctKeyAppendPlainColumn<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            return &distinctKeyAppendPlainColumn<types::Embedding::Primitive>;
        break;

        case NLChunkKind::List:
            return &distinctKeyAppendListColumn;
        break;

        case NLChunkKind::Map:
            return &distinctKeyAppendMapColumn;
        break;

        case NLChunkKind::Path:
            return &distinctKeyAppendPathColumn;
        break;

        case NLChunkKind::DateTime:
            return &distinctKeyAppendPlainColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &distinctKeyAppendPlainColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &distinctKeyAppendPlainColumn<types::Point::Primitive>;
        break;

        case NLChunkKind::PathRef:
            throw IRException("A path column cannot be a DISTINCT or grouping key: expand it into its list first");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLKeyAppendFunction NLExecutor::selectOptKeyAppendFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return &distinctKeyAppendOptColumn<EdgeTypeID>;
        break;

        case NLChunkKind::LabelID:
            return &distinctKeyAppendOptColumn<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &distinctKeyAppendOptColumn<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &distinctKeyAppendOptColumn<ValueType>;
        break;

        case NLChunkKind::Int64:
            return &distinctKeyAppendOptColumn<types::Int64::Primitive>;
        break;

        case NLChunkKind::UInt64:
            return &distinctKeyAppendOptColumn<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &distinctKeyAppendOptColumn<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &distinctKeyAppendOptColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &distinctKeyAppendOptColumn<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            return &distinctKeyAppendOptColumn<types::Embedding::Primitive>;
        break;

        case NLChunkKind::List:
            return &distinctKeyAppendOptListColumn;
        break;

        case NLChunkKind::Map:
            return &distinctKeyAppendOptMapColumn;
        break;

        case NLChunkKind::DateTime:
            return &distinctKeyAppendOptColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &distinctKeyAppendOptColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &distinctKeyAppendOptColumn<types::Point::Primitive>;
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("A node, an edge or a path column holds its own null, so no nullable chunk wraps one");
        break;

        case NLChunkKind::LabelSetID:
            throw IRException("A label set column is read as rows, so no nullable chunk wraps one");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLKeyPacking NLExecutor::selectKeyPacking(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return keyPacking<NodeID>();
        break;

        case NLChunkKind::EdgeID:
            return keyPacking<EdgeID>();
        break;

        case NLChunkKind::EdgeTypeID:
            return keyPacking<EdgeTypeID>();
        break;

        case NLChunkKind::LabelSetID:
            return keyPacking<LabelSetID>();
        break;

        case NLChunkKind::LabelID:
            return keyPacking<LabelID>();
        break;

        case NLChunkKind::PropertyTypeID:
            return keyPacking<PropertyTypeID>();
        break;

        case NLChunkKind::ValueTypeCode:
            return keyPacking<ValueType>();
        break;

        case NLChunkKind::UInt64:
            return keyPacking<types::UInt64::Primitive>();
        break;

        case NLChunkKind::Int64:
            return keyPacking<types::Int64::Primitive>();
        break;

        case NLChunkKind::Double:
            return keyPacking<types::Double::Primitive>();
        break;

        case NLChunkKind::Bool:
            return keyPacking<types::Bool::Primitive>();
        break;

        case NLChunkKind::DateTime:
            return keyPacking<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return keyPacking<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return keyPacking<types::Point::Primitive>();
        break;

        case NLChunkKind::String:
        case NLChunkKind::Embedding:
        case NLChunkKind::List:
        case NLChunkKind::Map:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            return NLKeyPacking {};
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return NLKeyPacking {};
}

NLKeyPacking NLExecutor::selectOptKeyPacking(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return optKeyPacking<EdgeTypeID>();
        break;

        case NLChunkKind::LabelID:
            return optKeyPacking<LabelID>();
        break;

        case NLChunkKind::PropertyTypeID:
            return optKeyPacking<PropertyTypeID>();
        break;

        case NLChunkKind::ValueTypeCode:
            return optKeyPacking<ValueType>();
        break;

        case NLChunkKind::Int64:
            return optKeyPacking<types::Int64::Primitive>();
        break;

        case NLChunkKind::UInt64:
            return optKeyPacking<types::UInt64::Primitive>();
        break;

        case NLChunkKind::Double:
            return optKeyPacking<types::Double::Primitive>();
        break;

        case NLChunkKind::Bool:
            return optKeyPacking<types::Bool::Primitive>();
        break;

        case NLChunkKind::DateTime:
            return optKeyPacking<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return optKeyPacking<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return optKeyPacking<types::Point::Primitive>();
        break;

        case NLChunkKind::String:
        case NLChunkKind::Embedding:
        case NLChunkKind::List:
        case NLChunkKind::Map:
            return NLKeyPacking {};
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("A node, an edge or a path column holds its own null, so no nullable chunk wraps one");
        break;

        case NLChunkKind::LabelSetID:
            throw IRException("A label set column is read as rows, so no nullable chunk wraps one");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return NLKeyPacking {};
}

NLJoinKeyFunctions NLExecutor::selectJoinKeyFunctions(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return joinKeyFunctions<NodeID>();
        break;

        case NLChunkKind::EdgeID:
            return joinKeyFunctions<EdgeID>();
        break;

        case NLChunkKind::EdgeTypeID:
            return joinKeyFunctions<EdgeTypeID>();
        break;

        case NLChunkKind::LabelSetID:
            return joinKeyFunctions<LabelSetID>();
        break;

        case NLChunkKind::LabelID:
            return joinKeyFunctions<LabelID>();
        break;

        case NLChunkKind::PropertyTypeID:
            return joinKeyFunctions<PropertyTypeID>();
        break;

        case NLChunkKind::ValueTypeCode:
            return joinKeyFunctions<ValueType>();
        break;

        case NLChunkKind::UInt64:
            return joinKeyFunctions<types::UInt64::Primitive>();
        break;

        case NLChunkKind::Int64:
            return joinKeyFunctions<types::Int64::Primitive>();
        break;

        case NLChunkKind::Double:
            return joinKeyFunctions<types::Double::Primitive>();
        break;

        case NLChunkKind::Bool:
            return joinKeyFunctions<types::Bool::Primitive>();
        break;

        case NLChunkKind::String:
            return joinKeyFunctions<types::String::Primitive>();
        break;

        case NLChunkKind::Embedding:
            return joinKeyFunctions<types::Embedding::Primitive>();
        break;

        case NLChunkKind::List:
            return joinKeyFunctions<ListView>();
        break;

        case NLChunkKind::Map:
            return joinKeyFunctions<MapView>();
        break;

        case NLChunkKind::Path:
            throw IRException("A path column cannot be a join key: a path has no scalar value to key on");
        break;

        case NLChunkKind::DateTime:
            return joinKeyFunctions<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return joinKeyFunctions<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return joinKeyFunctions<types::Point::Primitive>();
        break;

        case NLChunkKind::PathRef:
            throw IRException("A path column cannot be a join key: expand it into its list first");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return {};
}

NLJoinKeyFunctions NLExecutor::selectOptJoinKeyFunctions(NLChunkKind kind) {
    NLJoinKeyFunctions functions;
    dispatchOptChunkKind(kind, [&]<typename Element>() { functions = optJoinKeyFunctions<Element>(); });

    return functions;
}

NLJoinKeyFunctions NLExecutor::selectCellJoinKeyFunctions() {
    return joinKeyFunctions<CellView>();
}

// The pairs `=` compares are the pairs its kernel takes, so a pair the kernel has no
// comparison for never reaches a join: the filter it replaces could not have run.
NLKeyEqualFunction NLExecutor::selectCrossTypeKeyEqualFunction(const Column* probe, const Column* build) {
    using Pairs = PairRestrictions<OP_EQUAL>;

    CrossTypeKeyEqualSelector selector;
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           CrossTypeKeyEqualSelector,
                           typename Pairs::Excluded>::dispatch(probe, build, selector);

    if (!selector._equal) {
        throw IRException("A hash join key must be a column holding one value per row");
    }

    return selector._equal;
}

// An ID chunk (node/edge/edge-type IDs) has no null rows, so every row counts,
// regardless of kind. The count sibling of selectKeyAppendFunction, but kind is
// irrelevant here - the row count is just the column size.
size_t NLExecutor::countAllRows(const Column* column) {
    return column->size();
}

// Selected per column from its kind, so count(n) tallies only the rows in which the node
// or edge n is not null. A kind whose rows are never null - a label or edge-type ID, or a
// scalar a procedure yielded - has no invalid value to skip and gets no handle here.
NLCountFunction NLExecutor::selectIDCountFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &countValidIDs<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &countValidIDs<EdgeID>;
        break;

        default:
            return nullptr;
        break;
    }

    return nullptr;
}

// The grouped sibling of selectIDCountFunction, for count(n) inside a GROUP BY.
NLGroupAggregateFoldFunction NLExecutor::selectGroupCountValidIDFold(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &groupFoldCountValidID<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &groupFoldCountValidID<EdgeID>;
        break;

        default:
            return nullptr;
        break;
    }

    return nullptr;
}

NLCountFunction NLExecutor::selectPathCountFunction() {
    return &countPresentPaths;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountPresentPathFold() {
    return &groupFoldCountPresentPath;
}

NLAggregateFoldFunction NLExecutor::selectAggregateFold(AggregateKind kind, ValueType inputType) {
    switch (kind) {
        case AggregateKind::Sum:
            return selectSumFold(inputType);
        break;

        case AggregateKind::Avg:
            return selectAvgFold(inputType);
        break;

        case AggregateKind::StDev:
        case AggregateKind::StDevP:
            return selectDeviationFold(inputType);
        break;

        case AggregateKind::Min:
            return selectMinMaxFold</*IsMax=*/false>(inputType);
        break;

        case AggregateKind::Max:
            return selectMinMaxFold</*IsMax=*/true>(inputType);
        break;
    }

    bioassert(false, "Unhandled aggregate kind");
    return nullptr;
}

NLAggregateFoldFunction NLExecutor::selectTaggedAggregateFold(AggregateKind kind) {
    return taggedAggregateFoldFor(kind);
}

void NLExecutor::selectGroupAggregateState(GroupAggregateKind kind,
                                           ValueType accumulatorType,
                                           NLGroupAggregateStateType& stateType) {
    // count keeps only a tally, so it ignores the accumulator type; avg carries a running
    // total - an f64 over numbers, a 128-bit total over durations - and its count. A switch
    // (not an if/else) over every kind so a new one is a compile error here rather than
    // silently taking the sum/min/max path.
    switch (kind) {
        case GroupAggregateKind::Count:
        case GroupAggregateKind::CountDistinct:
        case GroupAggregateKind::CountRows:
            describeGroupState<uint64_t>(&groupEmitCopy<uint64_t, ColumnVector<uint64_t>>, stateType);
        break;

        case GroupAggregateKind::Avg:
        case GroupAggregateKind::AvgDistinct:
            dispatchAverageTotal(accumulatorType, [&]<typename Total>() {
                if constexpr (std::is_same_v<Total, types::Duration::Primitive>) {
                    describeGroupState<NLAverageState<__int128>>(&groupEmitAverage<__int128>, stateType);
                } else {
                    describeGroupState<NLAverageState<Total>>(&groupEmitAverage<Total>, stateType);
                }
            });
        break;

        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevDistinct:
            describeGroupState<NLDeviationState>(&groupEmitDeviation</*Population=*/false>, stateType);
        break;

        case GroupAggregateKind::StDevP:
        case GroupAggregateKind::StDevPDistinct:
            describeGroupState<NLDeviationState>(&groupEmitDeviation</*Population=*/true>, stateType);
        break;

        case GroupAggregateKind::Sum:
        case GroupAggregateKind::SumDistinct:
            describeGroupSumState(accumulatorType, stateType);
        break;

        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max:
        case GroupAggregateKind::First: {
            // min/max/first keep the value itself, null until a group sees one. The kept
            // state compiles for any value type, and lowering has already validated the kind
            // / type pairing (and the fold selector re-checks it), so one dispatch over the
            // accumulator type suffices - the grouped sibling of selectAggregateReset.
            const auto select = [&]<SupportedType T>() {
                describeGroupKeptValueState<typename T::Primitive>(stateType);
            };
            ValueTypeDispatcher(accumulatorType).execute(select);
        }
        break;
    }
}

NLGroupAggregateFoldFunction NLExecutor::selectTaggedGroupAggregateFold(GroupAggregateKind kind) {
    return taggedGroupAggregateFoldFor(kind);
}

void NLExecutor::selectTaggedGroupAggregateState(GroupAggregateKind kind, NLGroupAggregateStateType& stateType) {
    switch (kind) {
        case GroupAggregateKind::Sum:
        case GroupAggregateKind::SumDistinct:
            describeGroupState<NLTaggedSum>(&groupEmitTaggedSum, stateType);
        break;

        case GroupAggregateKind::Avg:
        case GroupAggregateKind::AvgDistinct:
            describeGroupState<NLAverageState<NLTaggedSum>>(&groupEmitTaggedAverage, stateType);
        break;

        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max:
        case GroupAggregateKind::First:
            describeGroupKeptValueState<CellView>(stateType);
        break;

        case GroupAggregateKind::Count:
        case GroupAggregateKind::CountDistinct:
        case GroupAggregateKind::CountRows:
        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevP:
        case GroupAggregateKind::StDevDistinct:
        case GroupAggregateKind::StDevPDistinct:
            bioassert(false, "A tally or a spread of tagged cells keeps no cell");
        break;
    }
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupAggregateFold(GroupAggregateKind kind, ValueType inputType) {
    switch (kind) {
        case GroupAggregateKind::CountRows:
            return selectGroupCountAllFold();
        break;

        case GroupAggregateKind::CountDistinct:
            return selectGroupCountDistinctOptFold(chunkKindFromValueType(inputType));
        break;

        case GroupAggregateKind::Count:
            return selectGroupCountOptFold(chunkKindFromValueType(inputType));
        break;

        case GroupAggregateKind::Sum:
            return selectGroupSumFold(inputType);
        break;

        case GroupAggregateKind::SumDistinct:
            return selectGroupSumDistinctFold(inputType);
        break;

        case GroupAggregateKind::Avg:
            return selectGroupAvgFold(inputType);
        break;

        case GroupAggregateKind::AvgDistinct:
            return selectGroupAvgDistinctFold(inputType);
        break;

        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevP:
            return selectGroupDeviationFold</*Distinct=*/false>(inputType);
        break;

        case GroupAggregateKind::StDevDistinct:
        case GroupAggregateKind::StDevPDistinct:
            return selectGroupDeviationFold</*Distinct=*/true>(inputType);
        break;

        case GroupAggregateKind::Min:
            return selectGroupMinMaxFold</*IsMax=*/false>(inputType);
        break;

        case GroupAggregateKind::Max:
            return selectGroupMinMaxFold</*IsMax=*/true>(inputType);
        break;

        case GroupAggregateKind::First:
            return selectGroupFirstFold(inputType);
        break;
    }

    bioassert(false, "Unhandled group aggregate kind");
    return nullptr;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountCellFold() {
    return &groupFoldCountPresentCell;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountDistinctCellFold() {
    return &groupFoldCountDistinctCell;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountAllFold() {
    // count(*) over an ID chunk (never null): every row charges its group.
    return &groupFoldCountAll;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountDistinctOptFold(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return &groupFoldCountDistinctPresent<EdgeTypeID>;
        break;

        case NLChunkKind::LabelID:
            return &groupFoldCountDistinctPresent<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &groupFoldCountDistinctPresent<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &groupFoldCountDistinctPresent<ValueType>;
        break;

        case NLChunkKind::Int64:
            return &groupFoldCountDistinctPresent<types::Int64::Primitive>;
        break;

        case NLChunkKind::UInt64:
            return &groupFoldCountDistinctPresent<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &groupFoldCountDistinctPresent<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &groupFoldCountDistinctPresent<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &groupFoldCountDistinctPresent<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            return &groupFoldCountDistinctPresent<types::Embedding::Primitive>;
        break;

        case NLChunkKind::List:
            return &groupFoldCountDistinctPresent<ListView>;
        break;

        case NLChunkKind::Map:
            return &groupFoldCountDistinctPresent<MapView>;
        break;

        case NLChunkKind::DateTime:
            return &groupFoldCountDistinctPresent<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &groupFoldCountDistinctPresent<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &groupFoldCountDistinctPresent<types::Point::Primitive>;
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("A node, an edge or a path column holds its own null, so no nullable chunk wraps one");
        break;

        case NLChunkKind::LabelSetID:
            throw IRException("A label set column is read as rows, so no nullable chunk wraps one");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountDistinctChunkFold(NLChunkKind kind) {
    NLGroupAggregateFoldFunction fold = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { fold = &groupFoldCountDistinctChunk<ElementType>; });

    return fold;
}

NLGroupKeyGatherFunction NLExecutor::selectGroupKeyGather(NLChunkKind kind) {
    NLGroupKeyGatherFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &groupGatherAppendColumn<ElementType>; });

    return selected;
}

NLKeyAppendFunction NLExecutor::selectPlainMergeKeyAppendFunction(NLChunkKind kind, ValueType keyType) {
    switch (kind) {
        case NLChunkKind::UInt64:
            return selectNumericMergeKeyAppend<types::UInt64::Primitive, MergeColumnShape::Plain>(keyType);
        break;

        case NLChunkKind::Int64:
            return selectNumericMergeKeyAppend<types::Int64::Primitive, MergeColumnShape::Plain>(keyType);
        break;

        case NLChunkKind::Double:
            return selectNumericMergeKeyAppend<types::Double::Primitive, MergeColumnShape::Plain>(keyType);
        break;

        case NLChunkKind::Bool:
            throwUnlessKeyedAsItsOwnType(ValueType::Bool, keyType);
            return &mergeKeyAppendPlainColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            throwUnlessKeyedAsItsOwnType(ValueType::String, keyType);
            return &mergeKeyAppendPlainColumn<types::String::Primitive>;
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::EdgeTypeID:
        case NLChunkKind::LabelSetID:
        case NLChunkKind::LabelID:
        case NLChunkKind::PropertyTypeID:
        case NLChunkKind::ValueTypeCode:
        case NLChunkKind::Embedding:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("a MERGE pattern cannot constrain a property to this value");
        break;

        case NLChunkKind::List:
            throwUnlessKeyedAsItsOwnType(ValueType::List, keyType);
            return &mergeKeyAppendPlainColumn<ListView>;
        break;

        case NLChunkKind::Map:
            throwUnlessKeyedAsItsOwnType(ValueType::Map, keyType);
            return &mergeKeyAppendPlainColumn<MapView>;
        break;

        case NLChunkKind::DateTime:
            throwUnlessKeyedAsItsOwnType(ValueType::DateTime, keyType);
            return &mergeKeyAppendPlainColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            throwUnlessKeyedAsItsOwnType(ValueType::Duration, keyType);
            return &mergeKeyAppendPlainColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            throwUnlessKeyedAsItsOwnType(ValueType::Point, keyType);
            return &mergeKeyAppendPlainColumn<types::Point::Primitive>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLKeyAppendFunction NLExecutor::selectConstMergeKeyAppendFunction(ValueType valueType, ValueType keyType) {
    switch (valueType) {
        case ValueType::Int64:
            return selectNumericMergeKeyAppend<types::Int64::Primitive, MergeColumnShape::Const>(keyType);
        break;

        case ValueType::UInt64:
            return selectNumericMergeKeyAppend<types::UInt64::Primitive, MergeColumnShape::Const>(keyType);
        break;

        case ValueType::Double:
            return selectNumericMergeKeyAppend<types::Double::Primitive, MergeColumnShape::Const>(keyType);
        break;

        case ValueType::Bool:
            throwUnlessKeyedAsItsOwnType(ValueType::Bool, keyType);
            return &mergeKeyAppendConstColumn<types::Bool::Primitive>;
        break;

        case ValueType::String:
            throwUnlessKeyedAsItsOwnType(ValueType::String, keyType);
            return &mergeKeyAppendConstColumn<types::String::Primitive>;
        break;

        case ValueType::Embedding:
            throwUnlessKeyedAsItsOwnType(ValueType::Embedding, keyType);
            return &mergeKeyAppendConstColumn<types::Embedding::Primitive>;
        break;

        case ValueType::List:
            throwUnlessKeyedAsItsOwnType(ValueType::List, keyType);
            return &mergeKeyAppendConstColumn<ListView>;
        break;

        case ValueType::DateTime:
            throwUnlessKeyedAsItsOwnType(ValueType::DateTime, keyType);
            return &mergeKeyAppendConstColumn<types::DateTime::Primitive>;
        break;

        case ValueType::Duration:
            throwUnlessKeyedAsItsOwnType(ValueType::Duration, keyType);
            return &mergeKeyAppendConstColumn<types::Duration::Primitive>;
        break;

        case ValueType::Point:
            throwUnlessKeyedAsItsOwnType(ValueType::Point, keyType);
            return &mergeKeyAppendConstColumn<types::Point::Primitive>;
        break;

        case ValueType::Map:
            throwUnlessKeyedAsItsOwnType(ValueType::Map, keyType);
            return &mergeKeyAppendConstColumn<MapView>;
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("invalid MERGE property value type");
        break;
    }

    bioassert(false, "Unhandled value type");
    return nullptr;
}

NLKeyAppendFunction NLExecutor::selectOptMergeKeyAppendFunction(NLChunkKind kind, ValueType keyType) {
    switch (kind) {
        case NLChunkKind::Int64:
            return selectNumericMergeKeyAppend<types::Int64::Primitive, MergeColumnShape::Opt>(keyType);
        break;

        case NLChunkKind::UInt64:
            return selectNumericMergeKeyAppend<types::UInt64::Primitive, MergeColumnShape::Opt>(keyType);
        break;

        case NLChunkKind::Double:
            return selectNumericMergeKeyAppend<types::Double::Primitive, MergeColumnShape::Opt>(keyType);
        break;

        case NLChunkKind::Bool:
            throwUnlessKeyedAsItsOwnType(ValueType::Bool, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::String:
            throwUnlessKeyedAsItsOwnType(ValueType::String, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::Embedding:
            throwUnlessKeyedAsItsOwnType(ValueType::Embedding, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::List:
            throwUnlessKeyedAsItsOwnType(ValueType::List, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::Map:
            throwUnlessKeyedAsItsOwnType(ValueType::Map, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::DateTime:
            throwUnlessKeyedAsItsOwnType(ValueType::DateTime, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::Duration:
            throwUnlessKeyedAsItsOwnType(ValueType::Duration, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::Point:
            throwUnlessKeyedAsItsOwnType(ValueType::Point, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::EdgeTypeID:
        case NLChunkKind::LabelSetID:
        case NLChunkKind::LabelID:
        case NLChunkKind::PropertyTypeID:
        case NLChunkKind::ValueTypeCode:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("a MERGE pattern cannot constrain a property to this value");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLKeyAppendFunction NLExecutor::selectNullMergeKeyAppendFunction() {
    return &mergeKeyAppendConstNull;
}

NLCompareFunction NLExecutor::selectCompareFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &compareColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &compareColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &compareColumn<EdgeTypeID>;
        break;

        case NLChunkKind::LabelSetID:
            return &compareColumn<LabelSetID>;
        break;

        case NLChunkKind::LabelID:
            return &compareColumn<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &compareColumn<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &compareColumn<ValueType>;
        break;

        case NLChunkKind::UInt64:
            return &compareColumn<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Int64:
            return &compareColumn<types::Int64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &compareColumn<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &compareColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &compareColumn<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            throw IRException("cannot sort by an embedding column");
        break;

        case NLChunkKind::List:
            return &compareContainerColumn<ListView>;
        break;

        case NLChunkKind::Map:
            return &compareContainerColumn<MapView>;
        break;

        case NLChunkKind::Path:
            return &comparePathColumn;
        break;

        case NLChunkKind::DateTime:
            return &compareColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &compareColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &compareColumn<types::Point::Primitive>;
        break;

        case NLChunkKind::PathRef:
            throw IRException("A path column cannot be a sort key: expand it into its list first");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// A manual switch, not dispatchOptChunkKind, because an embedding has no order:
// dispatching would instantiate the comparator for std::span<const float>, which does not
// compile. The embedding case throws instead, so that instantiation is never named.
NLCompareFunction NLExecutor::selectOptCompareFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return &compareOptColumn<EdgeTypeID>;
        break;

        case NLChunkKind::LabelID:
            return &compareOptColumn<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &compareOptColumn<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &compareOptColumn<ValueType>;
        break;

        case NLChunkKind::Int64:
            return &compareOptColumn<types::Int64::Primitive>;
        break;

        case NLChunkKind::UInt64:
            return &compareOptColumn<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &compareOptColumn<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &compareOptColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &compareOptColumn<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            throw IRException("cannot sort by an embedding column");
        break;

        case NLChunkKind::List:
            return &compareOptContainerColumn<ListView>;
        break;

        case NLChunkKind::Map:
            return &compareOptContainerColumn<MapView>;
        break;

        case NLChunkKind::DateTime:
            return &compareOptColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &compareOptColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &compareOptColumn<types::Point::Primitive>;
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("A node, an edge or a path column holds its own null, so no nullable chunk wraps one");
        break;

        case NLChunkKind::LabelSetID:
            throw IRException("A label set column is read as rows, so no nullable chunk wraps one");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// Read one property of the current input chunk into a nullable value column.
// The with-null writer emits one value per input row (null where the row lacks
// it), so no row is dropped and the value column lines up with the input chunk.
// The PropertyTypeID was resolved from the name during translation.
template <typename ID, typename T>
void NLExecutor::runPropertyFetch(NLExecutionContext* context, NLFunctionData* data) {
    NLPropertyFetchData* fetchData = static_cast<NLPropertyFetchData*>(data);

    const GraphView& view = *context->getView();
    const PropertyTypeID propertyTypeID = fetchData->getPropertyTypeID();
    const auto* inputIDs = static_cast<const ColumnVector<ID>*>(fetchData->getInput());
    auto* output = static_cast<ColumnOptVector<typename T::Primitive>*>(fetchData->getOutput());

    GetPropertiesWithNullChunkWriter<ID, T> writer(view, propertyTypeID, inputIDs);
    writer.setOutput(output);
    writer.fill(inputIDs->size());

    // The fetch above read the graph, which holds this change's writes nowhere: a row
    // whose entity the change wrote names it by the ID it will commit as, which the graph
    // holds nothing under, and a row it merely updated still reads the value from before.
    const ColumnVector<CustomBool>* pending = fetchData->getPending();
    const bool allPending = fetchData->isAllPending();
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();

    if (!writeBuffer) {
        bioassert(!pending && !allPending,
                  "a property fetch over written entities requires an active write transaction");
        return;
    }

    NLWrittenValues& written = context->getWrittenValues();
    written.indexUpdates(writeBuffer);

    NLValueStore& fetchedValues = fetchData->fetchedValues();
    fetchedValues.clear();

    const bool isNode = std::is_same_v<ID, NodeID>;
    const size_t committedCount = isNode ? committedNodeCount(&view) : committedEdgeCount(&view);
    const size_t pendingCount = isNode ? writeBuffer->numPendingNodes() : writeBuffer->numPendingEdges();
    const PendingRows pendingRows(pending, allPending, committedCount, pendingCount);

    if (!pending && !allPending && !written.hasUpdates() && pendingCount == 0) {
        return;
    }

    auto& raw = output->getRaw();
    const auto& inputRaw = inputIDs->getRaw();
    for (size_t row = 0; row < raw.size(); row++) {
        if (pendingRows.has(row, inputRaw[row].getValue())) {
            raw[row] = readPendingEntityProperty<ID, T>(writeBuffer,
                                                        written,
                                                        fetchedValues,
                                                        &view,
                                                        inputRaw[row].getValue(),
                                                        propertyTypeID);
            continue;
        }

        const CommitWriteBuffer::SupportedTypeVariant* update = written.findUpdate(inputRaw[row], propertyTypeID);
        if (update) {
            raw[row] = written.read<T>(*update, fetchedValues);
        }
    }
}

// The translator selects among these by the value type the property resolves
// to, on the node or edge side; only these (ID, T) pairs are available as
// handlers.
template void NLExecutor::runPropertyFetch<NodeID, types::Int64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::UInt64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Double>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Bool>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::String>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Embedding>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::List>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::DateTime>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Duration>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Point>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Map>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Int64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::UInt64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Double>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Bool>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::String>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Embedding>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::List>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::DateTime>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Duration>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Point>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Map>(NLExecutionContext*, NLFunctionData*);

void NLExecutor::runGetNodeLabelSet(NLExecutionContext* context, NLFunctionData* data) {
    NLGetNodeLabelSetData* fetchData = static_cast<NLGetNodeLabelSetData*>(data);

    const GraphView& view = *context->getView();
    const ColumnNodeIDs* input = fetchData->getInput();
    ColumnLabelSetIDs* output = fetchData->getOutput();

    GetNodeLabelSetChunkWriter writer(view, input);
    writer.setLabelSetIDs(output);
    writer.fill(input->size());

    // The graph holds no label set for a node this change wrote: the write buffer holds the
    // handle the CREATE spelled, under the ID the commit will know the set by.
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    if (!writeBuffer || writeBuffer->numPendingNodes() == 0) {
        return;
    }

    const size_t committedCount = committedNodeCount(&view);
    const PendingRows pendingRows(nullptr, false, committedCount, writeBuffer->numPendingNodes());

    auto& raw = output->getRaw();
    const auto& inputRaw = input->getRaw();
    for (size_t row = 0; row < raw.size(); row++) {
        const uint64_t nodeID = inputRaw[row].getValue();
        if (!pendingRows.has(row, nodeID)) {
            continue;
        }

        raw[row] = writeBuffer->getPendingNode(nodeID - committedCount).labelsetHandle.getID();
    }
}

void NLExecutor::runGetEdgeTypes(NLExecutionContext* context, NLFunctionData* data) {
    NLGetEdgeTypesData* fetchData = static_cast<NLGetEdgeTypesData*>(data);

    const GraphView& view = *context->getView();
    const ColumnEdgeIDs* input = fetchData->getInput();
    ColumnEdgeTypes* output = fetchData->getOutput();

    GetEdgeTypesChunkWriter writer(view, input);
    writer.setEdgeTypes(output);
    writer.fill(input->size());

    // The graph holds no type for an edge this change wrote: the write buffer holds the one
    // the CREATE spelled, under the ID the commit will know the edge by.
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    if (!writeBuffer || writeBuffer->numPendingEdges() == 0) {
        return;
    }

    const size_t committedCount = committedEdgeCount(&view);
    const PendingRows pendingRows(nullptr, false, committedCount, writeBuffer->numPendingEdges());

    auto& raw = output->getRaw();
    const auto& inputRaw = input->getRaw();
    for (size_t row = 0; row < raw.size(); row++) {
        const uint64_t edgeID = inputRaw[row].getValue();
        if (!pendingRows.has(row, edgeID)) {
            continue;
        }

        raw[row] = writeBuffer->getPendingEdge(edgeID - committedCount).edgeType;
    }
}

template <typename MaskColumn>
void NLExecutor::runCheckLabelConstraint(NLExecutionContext* context, NLFunctionData* data) {
    NLCheckLabelConstraintData* checkData = static_cast<NLCheckLabelConstraintData*>(data);

    const ColumnLabelSetIDs* input = checkData->getInput();
    MaskColumn* output = static_cast<MaskColumn*>(checkData->getOutput());

    const size_t rowCount = input->size();
    output->resize(rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const LabelSetID id = (*input)[rowIndex];

        if constexpr (std::is_same_v<MaskColumn, ColumnOptVector<CustomBool>>) {
            if (!id.isValid()) {
                output->setNull(rowIndex);
                continue;
            }
        }

        output->set(rowIndex, CustomBool {checkData->isMatching(id)});
    }
}

template void NLExecutor::runCheckLabelConstraint<ColumnVector<CustomBool>>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runCheckLabelConstraint<ColumnOptVector<CustomBool>>(NLExecutionContext*, NLFunctionData*);

template <typename MaskColumn>
void NLExecutor::runCheckEdgeTypeConstraint(NLExecutionContext* context, NLFunctionData* data) {
    NLCheckEdgeTypeConstraintData* checkData = static_cast<NLCheckEdgeTypeConstraintData*>(data);

    const ColumnEdgeTypes* input = checkData->getInput();
    MaskColumn* output = static_cast<MaskColumn*>(checkData->getOutput());

    const size_t rowCount = input->size();
    output->resize(rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const EdgeTypeID id = (*input)[rowIndex];

        if constexpr (std::is_same_v<MaskColumn, ColumnOptVector<CustomBool>>) {
            if (!id.isValid()) {
                output->setNull(rowIndex);
                continue;
            }
        }

        output->set(rowIndex, CustomBool {checkData->isMatching(id)});
    }
}

template void NLExecutor::runCheckEdgeTypeConstraint<ColumnVector<CustomBool>>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runCheckEdgeTypeConstraint<ColumnOptVector<CustomBool>>(NLExecutionContext*, NLFunctionData*);

template NLUnaryFn NLExecutor::selectNullTest<true>(const Column* operand, LocalMemory* memory, Column*& result);
template NLUnaryFn NLExecutor::selectNullTest<false>(const Column* operand, LocalMemory* memory, Column*& result);

template NLBinaryFn NLExecutor::selectBinary<OP_ADD>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_CONCAT>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_INDEX>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_SUB>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_MUL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_DIV>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_MOD>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_POW>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_EQUAL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_GREATER_THAN>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_LESS_THAN>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_GREATER_THAN_OR_EQUAL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_LESS_THAN_OR_EQUAL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_AND>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_OR>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_NOT_EQUAL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_XOR>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_STARTS_WITH>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_ENDS_WITH>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_CONTAINS>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_IN>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_INDEX_OF>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_FUNC_COSINE_SIMILARITY>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_FUNC_EUCLIDEAN_DISTANCE>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
