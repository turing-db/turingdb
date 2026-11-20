#pragma once

#include <stddef.h>
#include <stdint.h>
#include <concepts>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

#include "PathView.h"

#include "Bitmask.h"

namespace net::proto {

// The member-function contract the decode functions (Decoders.h) impose on the column
// classes they write into;
template <typename Column, typename T>
concept VectorColumn = requires(Column column, size_t count, T value) {
    { column.resize(count) };
    { column.reserve(count) };
    { column.size() } -> std::convertible_to<size_t>;
    { column.data() } -> std::same_as<T*>;
    { column[count] } -> std::same_as<T&>;
    { column.emplace_back(std::move(value)) } -> std::same_as<T&>;
};

template <typename Column, typename T>
concept ConstColumn = requires(Column column, const T& constValue, T value) {
    { column.set(constValue) };
    { column.set(std::move(value)) };
};

template <typename Column, typename T>
concept OptionalVectorColumn = requires(Column column, const Column constColumn, size_t count, const T& constValue, T value) {
    { column.resize(count) };
    { constColumn.isValid(count) } -> std::convertible_to<bool>;
    { column.set(count, constValue) };
    { column.set(count, std::move(value)) };
};

template <typename Column, typename T>
concept OptionalBufferColumn = requires(Column column, const DynamicLargeBitMask<uint64_t>& validity) {
    { column.getChunkValues() } -> std::same_as<T*>;
    { column.setValidity(validity) };
};

template <typename Column, typename T>
concept OptionalConstColumn = ConstColumn<Column, T>
&& requires(Column column, const std::optional<T>& constValue, std::optional<T> value) {
    { column.setOptional(constValue) };
    { column.setOptional(std::move(value)) };
};

// The value functions the decode layer calls on a sink's value store: the string,
// embedding and path allocator, and the container builder surface.
template <typename ValueStore, typename ListView, typename CellView, typename MapView>
concept ProtoDecodeValueStore = requires(ValueStore values,
                                         size_t count,
                                         char* bytes,
                                         float* floats,
                                         uint64_t* pathIDs,
                                         uint64_t scalarValue,
                                         std::string_view stringValue,
                                         std::span<const float> embeddingValue,
                                         db::PathView pathValue) {
    { values.allocString(count) } -> std::same_as<char*>;
    { values.getStringView(bytes, count) } -> std::same_as<std::string_view>;
    { values.allocEmbedding(count) } -> std::same_as<float*>;
    { values.getEmbeddingView(floats, count) } -> std::same_as<std::span<const float>>;
    { values.allocPathIDs(count) } -> std::same_as<uint64_t*>;
    { values.getPathView(pathIDs, count) } -> std::same_as<db::PathView>;

    { values.beginList(count, count) } -> std::same_as<ListView>;
    { values.beginNestedList(count, count) } -> std::same_as<CellView>;
    { values.writeListValue(stringValue) } -> std::same_as<CellView>;
    { values.writeListValue(embeddingValue) } -> std::same_as<CellView>;
    { values.writeListValue(pathValue) } -> std::same_as<CellView>;
    { values.writeCellBytes(bytes, count) } -> std::same_as<CellView>;

    { values.beginMap(count, count) } -> std::same_as<MapView>;
    { values.beginNestedMap(count, count) } -> std::same_as<CellView>;
    { values.writeMapKey(stringValue) };
    { values.writeMapValue(scalarValue) };
    { values.writeMapValue(stringValue) };
    { values.writeMapValue(embeddingValue) };
    { values.writeMapValue(pathValue) };
    { values.writeMapValueBytes(bytes, count) };
    { values.topMapExpectsValue() } -> std::convertible_to<bool>;

    // The open-container stack: one stack for both kinds, since a map can hold a list and
    // a list a map, so which container is innermost has to be unambiguous.
    { values.hasOpenContainer() } -> std::convertible_to<bool>;
    { values.topContainerIsMap() } -> std::convertible_to<bool>;
    { values.topContainerComplete() } -> std::convertible_to<bool>;
    { values.popContainer() };
    { values.openContainerCount() } -> std::convertible_to<size_t>;
    { values.topLevelValuesWritten() } -> std::convertible_to<size_t>;
    { values.reset() };
};

// The whole contract the decode layer places on a sink family:
// - the column aliases (probed with one representative element type)
// - the family's container handle types
// - the column container the decoded columns land in
// - the value store the values are written to
template <typename Sink>
concept ProtoDecodeSink = VectorColumn<typename Sink::template ColumnVector<uint64_t>, uint64_t>
&& OptionalVectorColumn<typename Sink::template ColumnOptVector<uint64_t>, uint64_t>
&& ConstColumn<typename Sink::template ColumnConst<uint64_t>, uint64_t>
&& OptionalConstColumn<typename Sink::template ColumnOptConst<uint64_t>, uint64_t>
&& ProtoDecodeValueStore<typename Sink::ValueStore,
                         typename Sink::ListView,
                         typename Sink::CellView,
                         typename Sink::MapView>
&& requires(Sink sink,
            typename Sink::ColumnContainer container,
            typename Sink::Column* column,
            size_t count,
            std::string_view columnName) {
    { sink.template alloc<typename Sink::template ColumnVector<uint64_t>>() } -> std::same_as<typename Sink::Column*>;
    { sink.getValues() } -> std::same_as<typename Sink::ValueStore*>;

    { container.size() } -> std::convertible_to<size_t>;
    { container[count] } -> std::same_as<typename Sink::Column*>;
    { container.addColumn(column, columnName) };
};

// Named handles for the sink's family-specific member types, so the decode layer can spell
// them without the typename/template noise.
template <typename T, ProtoDecodeSink Sink>
using SinkColumnVector = typename Sink::template ColumnVector<T>;

template <typename T, ProtoDecodeSink Sink>
using SinkColumnOptVector = typename Sink::template ColumnOptVector<T>;

template <typename T, ProtoDecodeSink Sink>
using SinkColumnConst = typename Sink::template ColumnConst<T>;

template <typename T, ProtoDecodeSink Sink>
using SinkColumnOptConst = typename Sink::template ColumnOptConst<T>;

template <ProtoDecodeSink Sink>
using SinkColumn = typename Sink::Column;

template <ProtoDecodeSink Sink>
using SinkColumnContainer = typename Sink::ColumnContainer;

template <ProtoDecodeSink Sink>
using SinkValueStore = typename Sink::ValueStore;

template <ProtoDecodeSink Sink>
using SinkListView = typename Sink::ListView;

template <ProtoDecodeSink Sink>
using SinkCellView = typename Sink::CellView;

template <ProtoDecodeSink Sink>
using SinkMapView = typename Sink::MapView;

// Satisfied when Column is one of the sink's const column families for element type T.
// Sink is unconstrained here because a concept's own parameters cannot carry constraints;
// it is always used with a ProtoDecodeSink.
template <typename Column, typename T, typename Sink>
concept ConstColumnOf = std::is_same_v<Column, SinkColumnConst<T, Sink>>
|| std::is_same_v<Column, SinkColumnOptConst<T, Sink>>;

}
