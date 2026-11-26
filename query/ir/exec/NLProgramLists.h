#pragma once

#include <stddef.h>
#include <stdint.h>
#include <optional>
#include <string_view>
#include <tuple>
#include <variant>
#include <vector>

#include "CellTag.h"
#include "ID.h"
#include "NLStepBuffers.h"
#include "NLValueStore.h"
#include "columns/ColumnVector.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "map/MapView.h"
#include "metadata/DateTime.h"
#include "metadata/Duration.h"
#include "metadata/Point.h"
#include "metadata/PropertyType.h"

#include "NLFunctionData.h"
#include "NLColumnFunctions.h"
#include "NLProgramExpressions.h"

namespace db {

// Whether the cell one column holds at @param row holds no list at all - a null, not an
// empty one. One per source column shape, selected during translation.
using NLCellAbsentFunction = bool (*)(const Column* source, size_t row);

// Copy every view a column holds - or only those of @param rows - into @param values, so
// the store they were copied into before can be emptied
using NLReownFunction = void (*)(Column* column, NLValueStore& values);
using NLReownRowsFunction = void (*)(Column* column, const std::vector<size_t>& rows, NLValueStore& values);

// Row-wise list build (nl.make_list): row r of the result is the list of row r of every
// element column, written into the query's list buffer as one contiguous run. Every
// element column carries rows and they are row-aligned, so the first of them gives the
// rows this step writes.
class NLMakeListData : public NLFunctionData {
public:
    struct Element {
        const Column* _column {nullptr};
        NLListItemReadFunction _read {nullptr};
    };

    NLMakeListData(Column* result, NLStepBuffers* stepBuffers)
        : _result(result),
        _stepBuffers(stepBuffers)
    {
    }

    Column* getResult() const { return _result; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

    const std::vector<Element>& elements() const { return _elements; }

    void addElement(const Element& element) {
        _elements.push_back(element);
    }

private:
    Column* _result {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
    std::vector<Element> _elements;
};

// Read the cell one value column holds at @param row as the value the map buffer stores
// for it. The map sibling of NLListItemReadFunction.
using NLMapValueReadFunction = MapBuffer<>::MapItemVariant (*)(const Column* input,
                                                              size_t row,
                                                              NLStepBuffers* stepBuffers);

// Row-wise map build (nl.make_map): row r of the result maps each key to row r of its
// value column, written into the query's map buffer as one contiguous run.
class NLMakeMapData : public NLFunctionData {
public:
    struct Entry {
        std::string_view _key;
        const Column* _column {nullptr};
        NLMapValueReadFunction _read {nullptr};
    };

    NLMakeMapData(Column* result, NLStepBuffers* stepBuffers);
    ~NLMakeMapData() override;

    Column* getResult() const { return _result; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

    const std::vector<Entry>& entries() const { return _entries; }

    void addEntry(const Entry& entry);

private:
    Column* _result {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
    std::vector<Entry> _entries;
};

// A row holding a value no key reads into: a number, a string, a boolean, an embedding or a
// list
struct NLKeylessValue {
    explicit NLKeylessValue(CellTag tag);

    CellTag _tag {CellTag::INVALID};
};

// What one row of an nl.static_map_key or an nl.dynamic_map_key reads its key in: a map, the
// node or the edge whose properties are its keys, or the instant, the duration or the point
// whose components are. Nothing where the row holds null.
using NLKeyedValue = std::variant<std::monostate, MapView, NodeID, EdgeID, DateTime, Duration, Point, NLKeylessValue>;

// The keyed value one column of a map key read holds at @param row. One per column shape,
// selected during translation.
using NLKeyedReadFunction = NLKeyedValue (*)(const Column* input, size_t row);

// The key one column of an nl.dynamic_map_key holds at @param row, or nothing where the
// row holds none. One per string column shape, selected during translation.
using NLStringReadFunction = std::optional<types::String::Primitive> (*)(const Column* input,
                                                                        size_t row);

// Row-wise map key read (nl.static_map_key): row r of the result views the value row r's map
// holds under the key, or a null cell where it holds none. The components of an instant, a
// duration and a point the key names are resolved during translation.
class NLStaticMapKeyData : public NLFunctionData {
public:
    // The part the key names of each type with components, as the row's value picks the type
    using ComponentParts = std::tuple<std::optional<DateTimePart>, std::optional<DurationPart>, std::optional<PointPart>>;

    NLStaticMapKeyData(const Column* input,
                 Column* result,
                 NLKeyedReadFunction keyedRead,
                 std::string_view key,
                 const ComponentParts& parts,
                 NLStepBuffers* stepBuffers)
        : _input(input),
        _result(result),
        _keyedRead(keyedRead),
        _key(key),
        _parts(parts),
        _stepBuffers(stepBuffers)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getResult() const { return _result; }
    NLKeyedReadFunction getKeyedRead() const { return _keyedRead; }
    std::string_view getKey() const { return _key; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

    template <typename Part>
    std::optional<Part> getPart() const { return std::get<std::optional<Part>>(_parts); }

private:
    const Column* _input {nullptr};
    Column* _result {nullptr};
    NLKeyedReadFunction _keyedRead {nullptr};
    std::string_view _key {};
    ComponentParts _parts;
    NLStepBuffers* _stepBuffers {nullptr};
};

// Row-wise map key read (nl.dynamic_map_key): row r of the result views the value row r's
// map holds under row r's key, or a null cell where it holds none.
class NLDynamicMapKeyData : public NLFunctionData {
public:
    NLDynamicMapKeyData(const Column* input,
                        Column* result,
                        NLKeyedReadFunction keyedRead,
                        const Column* key,
                        NLStringReadFunction keyRead,
                        NLStepBuffers* stepBuffers)
        : _input(input),
        _result(result),
        _keyedRead(keyedRead),
        _key(key),
        _keyRead(keyRead),
        _stepBuffers(stepBuffers)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getResult() const { return _result; }
    NLKeyedReadFunction getKeyedRead() const { return _keyedRead; }
    const Column* getKey() const { return _key; }
    NLStringReadFunction getKeyRead() const { return _keyRead; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

private:
    const Column* _input {nullptr};
    Column* _result {nullptr};
    NLKeyedReadFunction _keyedRead {nullptr};
    const Column* _key {nullptr};
    NLStringReadFunction _keyRead {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
};

// What one row of an nl.container_index's base holds, as each of its two reads sees it: the
// list a position reads, and what a key reads into, which is nothing where the row is null
struct NLContainer {
    std::optional<ListView> _list;
    NLKeyedValue _keyed;
};

using NLContainerReadFunction = NLContainer (*)(const Column* base, size_t row);

struct NLNeitherPositionNorKey {
    explicit NLNeitherPositionNorKey(CellTag tag);

    CellTag _tag {CellTag::INVALID};
};

using NLSubscript = std::variant<std::monostate,
                                 types::Int64::Primitive,
                                 types::UInt64::Primitive,
                                 types::String::Primitive,
                                 NLNeitherPositionNorKey>;

using NLSubscriptReadFunction = NLSubscript (*)(const Column* index, size_t row);

class NLContainerIndexData : public NLFunctionData {
public:
    NLContainerIndexData(const Column* base,
                         NLContainerReadFunction baseRead,
                         const Column* index,
                         NLSubscriptReadFunction indexRead,
                         Column* result,
                         NLStepBuffers* stepBuffers);
    ~NLContainerIndexData() override;

    const Column* getBase() const { return _base; }
    NLContainerReadFunction getBaseRead() const { return _baseRead; }
    const Column* getIndex() const { return _index; }
    NLSubscriptReadFunction getIndexRead() const { return _indexRead; }
    Column* getResult() const { return _result; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

private:
    const Column* _base {nullptr};
    NLContainerReadFunction _baseRead {nullptr};
    const Column* _index {nullptr};
    NLSubscriptReadFunction _indexRead {nullptr};
    Column* _result {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
};

// Row-wise map merge (nl.merge_maps): row r of the result holds every key the operands' maps
// hold at row r, under the value of the last operand holding it.
class NLMergeMapsData : public NLFunctionData {
public:
    struct Operand {
        const Column* _column {nullptr};
        NLKeyedReadFunction _read {nullptr};
    };

    NLMergeMapsData(Column* result, NLStepBuffers* stepBuffers);
    ~NLMergeMapsData() override;

    Column* getResult() const { return _result; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

    const std::vector<Operand>& getOperands() const { return _operands; }

    void addOperand(const Operand& operand);

private:
    Column* _result {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
    std::vector<Operand> _operands;
};

// The list one column of an nl.list_slice holds at @param row, or nothing where the row
// holds none. One per list column kind, selected during translation.
using NLListReadFunction = std::optional<ListView> (*)(const Column* input, size_t row);

using NLPercentileReadFunction = std::optional<types::Double::Primitive> (*)(const Column* input, size_t row);

// nl.percentile_cont / nl.percentile_disc data: the list column with the read telling an
// absent cell from a list, the percentile with the read its number type takes, and the
// column the step fills.
class NLPercentileData : public NLFunctionData {
public:
    NLPercentileData(const Column* list,
                     NLListReadFunction listRead,
                     const Column* percentile,
                     NLPercentileReadFunction percentileRead,
                     Column* result)
        : _list(list),
        _listRead(listRead),
        _percentile(percentile),
        _percentileRead(percentileRead),
        _result(result)
    {
    }

    const Column* getList() const { return _list; }
    NLListReadFunction getListRead() const { return _listRead; }
    const Column* getPercentile() const { return _percentile; }
    NLPercentileReadFunction getPercentileRead() const { return _percentileRead; }
    Column* getResult() const { return _result; }

private:
    const Column* _list {nullptr};
    NLListReadFunction _listRead {nullptr};
    const Column* _percentile {nullptr};
    NLPercentileReadFunction _percentileRead {nullptr};
    Column* _result {nullptr};
};

// Row-wise list build (nl.range): row r of the result counts from row r of the start to
// row r of the end by row r of the step, written into the query's list buffer as one
// contiguous run. The step reads no column where the query gave none, which counts by 1.
class NLRangeData : public NLFunctionData {
public:
    using Bound = NLRangeBound;

    NLRangeData(Column* result,
                NLStepBuffers* stepBuffers,
                const Bound& start,
                const Bound& end,
                const Bound& step)
        : _result(result),
        _stepBuffers(stepBuffers),
        _start(start),
        _end(end),
        _step(step)
    {
    }

    Column* getResult() const { return _result; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

    const Bound& getStart() const { return _start; }
    const Bound& getEnd() const { return _end; }
    const Bound& getStep() const { return _step; }

private:
    Column* _result {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
    Bound _start;
    Bound _end;
    Bound _step;
};

// nl.list_slice data: the per-row read of the run of a list its bounds span. Holds the
// list column with the read telling an absent cell from a list, each bound with the read
// its column shape takes - absent where the query gave none - and the column of views the
// step fills. The elements are the ones the list holds, so nothing is copied.
class NLListSliceData : public NLFunctionData {
public:
    using Bound = NLRangeBound;

    NLListSliceData(const Column* list,
                    NLListReadFunction listRead,
                    const Bound& from,
                    const Bound& to,
                    Column* result)
        : _list(list),
        _listRead(listRead),
        _from(from),
        _to(to),
        _result(result)
    {
    }

    const Column* getList() const { return _list; }
    NLListReadFunction getListRead() const { return _listRead; }
    const Bound& getFrom() const { return _from; }
    const Bound& getTo() const { return _to; }
    Column* getResult() const { return _result; }

private:
    const Column* _list {nullptr};
    NLListReadFunction _listRead {nullptr};
    Bound _from;
    Bound _to;
    Column* _result {nullptr};
};

// nl.sort_multi data: the list column and the fields column, each with the read telling an
// absent cell from a list, the limit and the skip with the read their column shape takes,
// the buffers the sorted lists are written into and the column the step fills. An operand
// the query left out has no column.
class NLSortMultiData : public NLFunctionData {
public:
    NLSortMultiData(const Column* list,
                    NLListReadFunction listRead,
                    const Column* fields,
                    NLListReadFunction fieldsRead,
                    const NLRangeBound& limit,
                    const NLRangeBound& skip,
                    NLStepBuffers* stepBuffers,
                    Column* result)
        : _list(list),
        _listRead(listRead),
        _fields(fields),
        _fieldsRead(fieldsRead),
        _limit(limit),
        _skip(skip),
        _stepBuffers(stepBuffers),
        _result(result)
    {
    }

    const Column* getList() const { return _list; }
    NLListReadFunction getListRead() const { return _listRead; }
    const Column* getFields() const { return _fields; }
    NLListReadFunction getFieldsRead() const { return _fieldsRead; }
    const NLRangeBound& getLimit() const { return _limit; }
    const NLRangeBound& getSkip() const { return _skip; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }
    Column* getResult() const { return _result; }

private:
    const Column* _list {nullptr};
    NLListReadFunction _listRead {nullptr};
    const Column* _fields {nullptr};
    NLListReadFunction _fieldsRead {nullptr};
    NLRangeBound _limit;
    NLRangeBound _skip;
    NLStepBuffers* _stepBuffers {nullptr};
    Column* _result {nullptr};
};

// What an op computing over the elements of one row's list holds - nl.list_comprehension
// and nl.list_predicate. The source column with the handler counting the elements each
// of its cells contributes, the drain filling the element chunk from them and the one
// telling a cell holding no list from an empty one; the carry set gathered by each
// element's source row - the same NLCarriedColumn shape the unwind loop uses; the body
// computing over the elements; and what that body ends on, the row each element came
// from and the value it contributes. The scratch columns hold, per element of the current
// chunk, the source row it came from and the position of the element inside that row's
// cell; the row tag column holds that same row for the body to cut alongside the elements.
class NLElementBodyData : public NLFunctionData {
public:
    using CarriedColumns = std::vector<NLCarriedColumn>;

    // @param elementEmit and @param elementOutput are null when the source's cells are
    // themselves the elements, exactly as on NLUnwindLoopData.
    NLElementBodyData(const Column* source,
                      NLUnwindElementCountFunction elementCount,
                      NLUnwindElementEmitFunction elementEmit,
                      NLCellAbsentFunction cellAbsent,
                      Column* elementOutput,
                      ColumnVector<uint64_t>* rowTags)
        : _source(source),
        _elementCount(elementCount),
        _elementEmit(elementEmit),
        _cellAbsent(cellAbsent),
        _elementOutput(elementOutput),
        _rowTags(rowTags)
    {
    }

    const Column* getSource() const { return _source; }
    NLUnwindElementCountFunction getElementCountFunc() const { return _elementCount; }
    NLUnwindElementEmitFunction getElementEmitFunc() const { return _elementEmit; }
    NLCellAbsentFunction getCellAbsentFunc() const { return _cellAbsent; }
    Column* getElementOutput() const { return _elementOutput; }
    ColumnVector<uint64_t>* getRowTags() const { return _rowTags; }

    const CarriedColumns& carriedColumns() const { return _carriedColumns; }

    void addCarriedColumn(const NLCarriedColumn& carried) {
        _carriedColumns.push_back(carried);
    }

    const Column* getYieldedRowTags() const { return _yieldedRowTags; }
    const Column* getValue() const { return _value; }

    void setYield(const Column* yieldedRowTags, const Column* value) {
        _yieldedRowTags = yieldedRowTags;
        _value = value;
    }

    ColumnVector<size_t>* getRows() { return &_rows; }
    ColumnVector<size_t>* getPositions() { return &_positions; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    const Column* _source {nullptr};
    NLUnwindElementCountFunction _elementCount {nullptr};
    NLUnwindElementEmitFunction _elementEmit {nullptr};
    NLCellAbsentFunction _cellAbsent {nullptr};
    Column* _elementOutput {nullptr};
    ColumnVector<uint64_t>* _rowTags {nullptr};

    const Column* _yieldedRowTags {nullptr};
    const Column* _value {nullptr};

    CarriedColumns _carriedColumns;
    NLStmtContainer _stmts;

    ColumnVector<size_t> _rows;
    ColumnVector<size_t> _positions;
};

// nl.list_comprehension data: the per-row list build of `[x IN xs WHERE p(x) | f(x)]`.
// The value each element contributes is read under the read its column shape takes; the
// staged elements and their per-row counts hold what the whole step gathered, which the
// lists are built from once every element has been seen.
class NLListComprehensionData : public NLElementBodyData {
public:
    NLListComprehensionData(const Column* source,
                            NLUnwindElementCountFunction elementCount,
                            NLUnwindElementEmitFunction elementEmit,
                            NLCellAbsentFunction cellAbsent,
                            Column* elementOutput,
                            ColumnVector<uint64_t>* rowTags,
                            Column* result,
                            NLStepBuffers* stepBuffers)
        : NLElementBodyData(source, elementCount, elementEmit, cellAbsent, elementOutput, rowTags),
        _result(result),
        _stepBuffers(stepBuffers)
    {
    }

    Column* getResult() const { return _result; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

    NLListItemReadFunction getValueRead() const { return _valueRead; }
    void setValueRead(NLListItemReadFunction valueRead) { _valueRead = valueRead; }

    std::vector<ListBuffer<>::ListItemVariant>& stagedElements() { return _stagedElements; }

    // The copies of the staged elements, which the body's later runs would otherwise empty
    // the buffers of
    NLValueStore& stagedValues() { return _stagedValues; }
    std::vector<size_t>& stagedCounts() { return _stagedCounts; }

private:
    Column* _result {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
    NLListItemReadFunction _valueRead {nullptr};

    std::vector<ListBuffer<>::ListItemVariant> _stagedElements;
    NLValueStore _stagedValues;
    std::vector<size_t> _stagedCounts;
};

// How many elements of a list a predicate must hold for; the interpreter-side
// counterpart of the MLIR storage::ListPredicateKind, which the translator maps onto this
enum class ListPredicateKind {
    All,
    Any,
    None,
    Single,
};

// The truth value one element's predicate holds, absent where it is null
using NLTruthReadFunction = std::optional<bool> (*)(const Column* mask, size_t row);

// nl.list_predicate data: the per-row decision of `all(x IN xs WHERE p(x))` and its
// siblings. Each element's truth value is read off the yielded mask and tallied into the
// counts of its row, which decide the row once every element has been seen; the result
// is a nullable mask.
class NLListPredicateData : public NLElementBodyData {
public:
    NLListPredicateData(const Column* source,
                        NLUnwindElementCountFunction elementCount,
                        NLUnwindElementEmitFunction elementEmit,
                        NLCellAbsentFunction cellAbsent,
                        Column* elementOutput,
                        ColumnVector<uint64_t>* rowTags,
                        Column* result,
                        ListPredicateKind kind)
        : NLElementBodyData(source, elementCount, elementEmit, cellAbsent, elementOutput, rowTags),
        _result(result),
        _kind(kind)
    {
    }

    Column* getResult() const { return _result; }
    ListPredicateKind getKind() const { return _kind; }

    NLTruthReadFunction getTruthRead() const { return _truthRead; }
    void setTruthRead(NLTruthReadFunction truthRead) { _truthRead = truthRead; }

    std::vector<size_t>& trueCounts() { return _trueCounts; }
    std::vector<size_t>& falseCounts() { return _falseCounts; }
    std::vector<size_t>& nullCounts() { return _nullCounts; }

private:
    Column* _result {nullptr};
    ListPredicateKind _kind {ListPredicateKind::All};
    NLTruthReadFunction _truthRead {nullptr};

    std::vector<size_t> _trueCounts;
    std::vector<size_t> _falseCounts;
    std::vector<size_t> _nullCounts;
};

// The per-row fold of `reduce(acc = init, x IN xs | f(acc, x))`, nl.reduce's data and
// the part of nl.all_reduce's it shares. The accumulator, nl.reduce's result, is reset,
// takes the initial value of each row whose source cell holds a list, then the body's
// value once per element. The body runs once per element position over the rows whose
// list reaches it; the carried columns gather those rows' cells of the accumulator, of
// the source where its cells are the elements, and of the carry set.
class NLFoldData : public NLFunctionData {
public:
    using CarriedColumns = std::vector<NLCarriedColumn>;

    NLFoldData(const Column* source,
               NLUnwindElementCountFunction elementCount,
               NLUnwindElementEmitFunction elementEmit,
               NLCellAbsentFunction cellAbsent,
               Column* elementOutput,
               const Column* initialValue,
               Column* accumulator,
               NLStepBuffers* stepBuffers)
        : _source(source),
        _elementCount(elementCount),
        _elementEmit(elementEmit),
        _cellAbsent(cellAbsent),
        _elementOutput(elementOutput),
        _initialValue(initialValue),
        _accumulator(accumulator),
        _stepBuffers(stepBuffers)
    {
    }

    const Column* getSource() const { return _source; }
    NLUnwindElementCountFunction getElementCountFunc() const { return _elementCount; }
    NLUnwindElementEmitFunction getElementEmitFunc() const { return _elementEmit; }
    NLCellAbsentFunction getCellAbsentFunc() const { return _cellAbsent; }
    Column* getElementOutput() const { return _elementOutput; }
    const Column* getInitialValue() const { return _initialValue; }
    Column* getAccumulator() const { return _accumulator; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

    const CarriedColumns& carriedColumns() const { return _carriedColumns; }

    void addCarriedColumn(const NLCarriedColumn& carried) {
        _carriedColumns.push_back(carried);
    }

    const Column* getValue() const { return _value; }
    void setValue(const Column* value) { _value = value; }

    NLCaseResetFn getReset() const { return _reset; }
    NLCaseFinishFn getFinish() const { return _finish; }

    void setReset(NLCaseResetFn reset, NLCaseFinishFn finish) {
        _reset = reset;
        _finish = finish;
    }

    const NLCellWrite& getInitialWrite() const { return _initialWrite; }
    const NLCellWrite& getValueWrite() const { return _valueWrite; }

    void setWrites(const NLCellWrite& initialWrite, const NLCellWrite& valueWrite) {
        _initialWrite = initialWrite;
        _valueWrite = valueWrite;
    }

    ColumnVector<size_t>* getRows() { return &_rows; }
    ColumnVector<size_t>* getPositions() { return &_positions; }

    std::vector<size_t>& elementCounts() { return _elementCounts; }
    std::vector<size_t>& reachingRows() { return _reachingRows; }
    std::vector<size_t>& endedRows() { return _endedRows; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

    // The accumulators, copied in as they are written: the body computing the next one
    // empties the buffers the last one was built in. A tagged cell is built owned, so only
    // a typed accumulator of views has its written rows copied.
    NLCompactingValueStore& accumulatorValues() { return _accumulatorValues; }
    NLReownFunction getReown() const { return _reown; }
    NLReownRowsFunction getReownRows() const { return _reownRows; }

    void setReowns(NLReownFunction reown, NLReownRowsFunction reownRows) {
        _reown = reown;
        _reownRows = reownRows;
    }

private:
    const Column* _source {nullptr};
    NLUnwindElementCountFunction _elementCount {nullptr};
    NLUnwindElementEmitFunction _elementEmit {nullptr};
    NLCellAbsentFunction _cellAbsent {nullptr};
    Column* _elementOutput {nullptr};
    const Column* _initialValue {nullptr};
    Column* _accumulator {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
    const Column* _value {nullptr};
    NLCaseResetFn _reset {nullptr};
    NLCaseFinishFn _finish {nullptr};
    NLCellWrite _initialWrite;
    NLCellWrite _valueWrite;
    NLCompactingValueStore _accumulatorValues;
    NLReownFunction _reown {nullptr};
    NLReownRowsFunction _reownRows {nullptr};

    CarriedColumns _carriedColumns;
    NLStmtContainer _stmts;

    ColumnVector<size_t> _rows;
    ColumnVector<size_t> _positions;
    std::vector<size_t> _elementCounts;
    std::vector<size_t> _reachingRows;
    std::vector<size_t> _endedRows;
};

// nl.all_reduce data: nl.reduce's fold, whose accumulator is a column of its own, the
// predicate run after each step over the accumulators it wrote, and what the predicate
// answered for each row: false at some step, which ends the row's fold, or null at some
// step. The predicate reads the body's element and carried chunks; only the accumulator
// is gathered again.
class NLAllReduceData : public NLFoldData {
public:
    NLAllReduceData(const Column* source,
                    NLUnwindElementCountFunction elementCount,
                    NLUnwindElementEmitFunction elementEmit,
                    NLCellAbsentFunction cellAbsent,
                    Column* elementOutput,
                    const Column* initialValue,
                    Column* accumulator,
                    NLStepBuffers* stepBuffers,
                    Column* result)
        : NLFoldData(source, elementCount, elementEmit, cellAbsent, elementOutput, initialValue, accumulator, stepBuffers),
        _result(result)
    {
    }

    Column* getResult() const { return _result; }

    Column* getTestedAccumulator() const { return _testedAccumulator; }
    NLGatherFunction getAccumulatorGather() const { return _accumulatorGather; }

    void setTestedAccumulator(Column* testedAccumulator, NLGatherFunction accumulatorGather) {
        _testedAccumulator = testedAccumulator;
        _accumulatorGather = accumulatorGather;
    }

    NLStmtContainer* getPredicateStmts() { return &_predicateStmts; }
    const NLStmtContainer* getPredicateStmts() const { return &_predicateStmts; }

    const Column* getHolds() const { return _holds; }
    NLTruthReadFunction getTruthRead() const { return _truthRead; }

    void setHolds(const Column* holds, NLTruthReadFunction truthRead) {
        _holds = holds;
        _truthRead = truthRead;
    }

    std::vector<bool>& failedRows() { return _failedRows; }
    std::vector<bool>& unknownRows() { return _unknownRows; }

private:
    Column* _result {nullptr};
    Column* _testedAccumulator {nullptr};
    NLGatherFunction _accumulatorGather {nullptr};
    const Column* _holds {nullptr};
    NLTruthReadFunction _truthRead {nullptr};

    NLStmtContainer _predicateStmts;

    std::vector<bool> _failedRows;
    std::vector<bool> _unknownRows;
};

}
