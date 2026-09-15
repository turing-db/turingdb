#pragma once

#include <stddef.h>
#include <stdint.h>
#include <concepts>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

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
concept OptionalVectorColumn = VectorColumn<Column, std::optional<T>>;

template <typename Column, typename T>
concept OptionalConstColumn = ConstColumn<Column, std::optional<T>>;

// The whole contract the decode layer places on a sink family: the column aliases
// (probed with one representative element type), the family's container handle types, the
// string/embedding allocator, the container builder surface, and the column container the
// decoded columns land in.
template <typename Sink>
concept ProtoDecodeSink = VectorColumn<typename Sink::template ColumnVector<uint64_t>, uint64_t>
&& OptionalVectorColumn<typename Sink::template ColumnOptVector<uint64_t>, uint64_t>
&& ConstColumn<typename Sink::template ColumnConst<uint64_t>, uint64_t>
&& OptionalConstColumn<typename Sink::template ColumnOptConst<uint64_t>, uint64_t>
&& requires(Sink sink,
            typename Sink::ColumnContainer container,
            typename Sink::Column* column,
            size_t count,
            char* bytes,
            float* floats,
            uint64_t scalarValue,
            std::string_view stringValue,
            std::span<const float> embeddingValue,
            std::string_view columnName) {
    { sink.template alloc<typename Sink::template ColumnVector<uint64_t>>() } -> std::same_as<typename Sink::Column*>;

    { sink.allocString(count) } -> std::same_as<char*>;
    { sink.getStringView(bytes, count) } -> std::same_as<std::string_view>;
    { sink.allocEmbedding(count) } -> std::same_as<float*>;
    { sink.getEmbeddingView(floats, count) } -> std::same_as<std::span<const float>>;

    { sink.beginList(count, count) } -> std::same_as<typename Sink::ListView>;
    { sink.beginNestedList(count, count) } -> std::same_as<typename Sink::ListElementView>;
    { sink.writeListValue(stringValue) } -> std::same_as<typename Sink::ListElementView>;
    { sink.writeListValue(embeddingValue) } -> std::same_as<typename Sink::ListElementView>;
    { sink.writeListElementBytes(bytes, count) } -> std::same_as<typename Sink::ListElementView>;

    { sink.beginMap(count, count) } -> std::same_as<typename Sink::MapView>;
    { sink.beginNestedMap(count, count) };
    { sink.writeMapKey(stringValue) };
    { sink.writeMapValue(scalarValue) };
    { sink.writeMapValue(stringValue) };
    { sink.writeMapValue(embeddingValue) };
    { sink.writeMapValueBytes(bytes, count) };
    { sink.topMapExpectsValue() } -> std::convertible_to<bool>;

    // The open-container stack: one stack for both kinds, since a map can hold a list and
    // a list a map, so which container is innermost has to be unambiguous.
    { sink.hasOpenContainer() } -> std::convertible_to<bool>;
    { sink.topContainerIsMap() } -> std::convertible_to<bool>;
    { sink.topContainerComplete() } -> std::convertible_to<bool>;
    { sink.popContainer() };
    { sink.openContainerCount() } -> std::convertible_to<size_t>;
    { sink.topLevelValuesWritten() } -> std::convertible_to<size_t>;
    { sink.reset() };

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
using SinkListView = typename Sink::ListView;

template <ProtoDecodeSink Sink>
using SinkListElementView = typename Sink::ListElementView;

template <ProtoDecodeSink Sink>
using SinkMapView = typename Sink::MapView;

// Satisfied when Column is one of the sink's const column families for element type T.
// Sink is unconstrained here because a concept's own parameters cannot carry constraints;
// it is always used with a ProtoDecodeSink.
template <typename Column, typename T, typename Sink>
concept ConstColumnOf = std::is_same_v<Column, SinkColumnConst<T, Sink>>
|| std::is_same_v<Column, SinkColumnOptConst<T, Sink>>;

}
