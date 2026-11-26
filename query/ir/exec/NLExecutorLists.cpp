#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>
#include <cmath>
#include <functional>
#include <numeric>
#include <span>
#include <string_view>
#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "TypeUtils.h"
#include "ID.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOperators.h"
#include "CellOrder.h"
#include "QueryTypeName.h"
#include "map/MapUtils.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLColumnKernels.h"
#include "NLRangeSpan.h"
#include "NLTaggedCells.h"

#include "IRException.h"
#include "IRRange.h"
#include "BioAssert.h"

using namespace db;

namespace {

// Read one cell of a nullable value column as the value it contributes to a list or a map:
// the value it holds, or the tagged null Cypher leaves where the row has none.
template <typename Item, typename Primitive>
Item valueItem(const Column* input, size_t row, NLStepBuffers*) {
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);
    if (!typedInput->isValid(row)) {
        return Item {PropertyNull {}};
    }

    return Item {typedInput->getValue(row)};
}

// Read one cell of a nullable integer column as a bound of a range: the number it holds,
// or nothing where the row has none.
std::optional<ListView> plainListRead(const Column* input, size_t row) {
    return (*static_cast<const ColumnVector<ListView>*>(input))[row];
}

std::optional<ListView> optListRead(const Column* input, size_t row) {
    return static_cast<const ColumnOptVector<ListView>*>(input)->get(row);
}

std::optional<ListView> constListRead(const Column* input, size_t) {
    return static_cast<const ColumnConst<ListView>*>(input)->getRaw();
}

std::optional<ListView> optConstListRead(const Column* input, size_t) {
    return constantOperand(static_cast<const ColumnConst<std::optional<ListView>>*>(input));
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

template <typename Primitive>
std::optional<types::Int64::Primitive> rangeBound(const Column* input, size_t row) {
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);
    if (!typedInput->isValid(row)) {
        return std::nullopt;
    }

    return static_cast<types::Int64::Primitive>(typedInput->getValue(row));
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
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);
    if (!typedInput->isValid(row)) {
        return std::nullopt;
    }

    return static_cast<types::Double::Primitive>(typedInput->getValue(row));
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

    using OutputColumn = ColumnVectorOf<Result>;
    OutputColumn* output = static_cast<OutputColumn*>(data->getResult());
    resetForRowWrites(output, rowCount);

    std::vector<CellView> numbers;

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<ListView> values = listRead(list, rowIndex);
        if (values) {
            sortListNumbers(*values, numbers, functionName);
        } else {
            numbers.clear();
        }

        if (numbers.empty()) {
            writeRow(output, rowIndex, absentPercentile<Result>());
            continue;
        }

        const std::optional<double> fraction = percentileRead(percentile, rowIndex);
        if (!fraction) {
            throw IRException(fmt::format("{}() takes a percentile between 0.0 and 1.0, not null", functionName));
        }

        throwIfNotAPercentile(*fraction, functionName);
        writeRow(output, rowIndex, percentileOf(numbers, *fraction));
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
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    if (!typedInput->isValid(row)) {
        return Item {PropertyNull {}};
    }

    return Item {typedInput->getValue(row)};
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

void NLExecutor::runListSlice(NLExecutionContext*, NLFunctionData* data) {
    const NLListSliceData* slice = static_cast<NLListSliceData*>(data);

    const Column* const list = slice->getList();
    const NLListReadFunction listRead = slice->getListRead();
    const NLListSliceData::Bound& from = slice->getFrom();
    const NLListSliceData::Bound& to = slice->getTo();

    const size_t rowCount = list->size();

    ColumnOptVector<ListView>* output = static_cast<ColumnOptVector<ListView>*>(slice->getResult());
    output->clear();
    output->reserve(rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<ListView> elements = listRead(list, rowIndex);

        const std::optional<types::Int64::Primitive> lower =
            from._column ? from._read(from._column, rowIndex) : std::optional<types::Int64::Primitive> {0};

        const std::optional<types::Int64::Primitive> upper =
            to._column ? to._read(to._column, rowIndex) : std::optional<types::Int64::Primitive> {};

        const bool boundsPresent = lower.has_value() && (!to._column || upper.has_value());
        if (!elements || !boundsPresent) {
            output->pushNull();
            continue;
        }

        const int64_t size = static_cast<int64_t>(elements->size());
        const int64_t last = to._column ? *upper : size;

        // A bound counts from the end where it is negative, and is clamped to the list
        // where it runs past either end, so no row errors on a bound of its own
        const int64_t first = std::clamp(*lower < 0 ? size + *lower : *lower, int64_t {0}, size);
        const int64_t end = std::clamp(last < 0 ? size + last : last, int64_t {0}, size);

        const size_t count = end > first ? static_cast<size_t>(end - first) : 0;

        output->push_back(elements->slice(static_cast<size_t>(first), count));
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

    ColumnOptVector<ListView>* output = static_cast<ColumnOptVector<ListView>*>(sortMulti->getResult());
    resetForRowWrites(output, rowCount);

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
            output->setNull(rowIndex);
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
            output->set(rowIndex, elements->slice(first, count));
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

        output->set(rowIndex, listBuffer.gather(sorted));
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

    using OutputColumn = ColumnVectorOf<Result>;
    OutputColumn* output = static_cast<OutputColumn*>(merge->getResult());
    resetForRowWrites(output, rowCount);

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
                output->setNull(rowIndex);
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

        output->set(rowIndex, mapBuffer.insert(merged));
    }
}

template void NLExecutor::runMergeMaps<MapView>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runMergeMaps<std::optional<MapView>>(NLExecutionContext* context, NLFunctionData* data);

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

    ColumnOptVector<ListView>* output = static_cast<ColumnOptVector<ListView>*>(range->getResult());
    output->clear();
    output->reserve(rowCount);

    std::vector<ListBuffer<>::ListItemVariant> elements;
    NLRangeSpan span;

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (!span.read(start, end, step, rowIndex)) {
            output->pushNull();
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

        output->push_back(listBuffer.insert(elements));
    }
}

NLListItemReadFunction NLExecutor::selectValueListItemRead(NLChunkKind kind) {
    NLListItemReadFunction selected = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { selected = &valueItem<ListBuffer<>::ListItemVariant, Element>; });

    return selected;
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

NLListItemReadFunction NLExecutor::selectOptNestedListItemRead() {
    return &optItem<ListBuffer<>::ListItemVariant, ListView>;
}
