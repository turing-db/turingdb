#pragma once

#include <stddef.h>
#include <stdint.h>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "ChunkedBuffer.h"
#include "TuringProtoDecoderConcepts.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListUtils.h"
#include "map/MapBufferTypeTag.h"

#include "Column.h"
#include "ColumnConst.h"
#include "ColumnContainer.h"
#include "ColumnVector.h"
#include "ListElementView.h"
#include "ListView.h"

namespace wasm {

template <typename T>
struct ElementOf {
    using Type = T;
};

template <typename T>
struct ElementOf<std::optional<T>> {
    using Type = T;
};

// Maps a column's element type back to its wire type code, so sink-allocated columns
// are self-describing.
template <typename T>
consteval ColumnType wireCodeOfElement() {
    using Element = typename ElementOf<T>::Type;

    if constexpr (std::is_same_v<Element, db::types::UInt64::Primitive>) {
        return ColumnType::UINT64;
    } else if constexpr (std::is_same_v<Element, db::types::Int64::Primitive>) {
        return ColumnType::INT64;
    } else if constexpr (std::is_same_v<Element, db::types::Double::Primitive>) {
        return ColumnType::DOUBLE;
    } else if constexpr (std::is_same_v<Element, db::types::Bool::Primitive>) {
        return ColumnType::BOOL;
    } else if constexpr (std::is_same_v<Element, db::types::String::Primitive>) {
        return ColumnType::STRING;
    } else if constexpr (std::is_same_v<Element, db::types::Embedding::Primitive>) {
        return ColumnType::EMBEDDING;
    } else if constexpr (std::is_same_v<Element, db::types::DateTime::Primitive>) {
        return ColumnType::DATE_TIME;
    } else if constexpr (std::is_same_v<Element, db::Path>) {
        return ColumnType::PATH;
    } else if constexpr (std::is_same_v<Element, db::EntityList>) {
        return ColumnType::ENTITY_LIST;
    } else if constexpr (std::is_same_v<Element, ListView>) {
        return ColumnType::LIST_VIEW;
    } else if constexpr (std::is_same_v<Element, ListElementView>) {
        return ColumnType::LIST_ELEMENT_VIEW;
    } else if constexpr (std::is_same_v<Element, MapView>) {
        return ColumnType::MAP_VIEW;
    } else if constexpr (std::is_same_v<Element, db::ValueType>) {
        return ColumnType::VALUE_TYPE;
    } else if constexpr (std::is_same_v<Element, db::PropertyNull>) {
        return ColumnType::PROPERTY_NULL;
    } else if constexpr (std::is_same_v<Element, db::NodeID>) {
        return ColumnType::NODE_ID;
    } else if constexpr (std::is_same_v<Element, db::EdgeID>) {
        return ColumnType::EDGE_ID;
    } else if constexpr (std::is_same_v<Element, db::EdgeTypeID>) {
        return ColumnType::EDGE_TYPE_ID;
    } else if constexpr (std::is_same_v<Element, db::PropertyTypeID>) {
        return ColumnType::PROPERTY_TYPE_ID;
    } else if constexpr (std::is_same_v<Element, db::LabelID>) {
        return ColumnType::LABEL_ID;
    } else if constexpr (std::is_same_v<Element, db::LabelSetID>) {
        return ColumnType::LABEL_SET_ID;
    } else if constexpr (std::is_same_v<Element, db::ChangeID>) {
        return ColumnType::CHANGE_ID;
    } else {
        static_assert(sizeof(Element) == 0, "No wire type code for this element type");
    }
}

// Lists and maps are serialised depth-first into one flat little-endian byte buffer that JS
// reads directly: a list is [u32 elementCount] then its elements, an element [u8 tag] then
// its payload — 8 bytes for Int/UInt/Double/NodeID/EdgeID, 1 for Bool/Null, [u32 index] into
// the nested string set for String, [u32 byteSize][bytes] for Embedding, a nested list or map inline
// for ListView or MapView. A map is [u32 entryCount] then its entries, an entry [u32 index]
// of its key in the nested string set then its value, written exactly as a list element is.
class WasmSink {
public:
    using ColumnContainer = wasm::ColumnContainer;
    using Column = wasm::Column;

    template <typename T>
    using ColumnVector = wasm::ColumnVector<T>;

    template <typename T>
    using ColumnOptVector = wasm::ColumnVector<std::optional<T>>;

    template <typename T>
    using ColumnConst = wasm::ColumnConst<T>;

    template <typename T>
    using ColumnOptConst = wasm::ColumnConst<std::optional<T>>;

    using ListView = wasm::ListView;
    using ListElementView = wasm::ListElementView;
    using MapView = wasm::MapView;

    WasmSink();
    ~WasmSink();

    template <typename ColumnT>
    Column* alloc() { return new ColumnT(wireCodeOfElement<typename ColumnT::ValueType>()); }

    float* allocEmbedding(size_t numFloats);
    std::span<const float> getEmbeddingView(float* data, size_t numFloats);

    char* allocString(size_t size);
    std::string_view getStringView(char* data, size_t size);

    ListView beginList(size_t elementCount, size_t byteSize);
    ListElementView beginNestedList(size_t elementCount, size_t byteSize);
    ListElementView writeListValue(std::string_view value);
    ListElementView writeListValue(std::span<const float> value);
    ListElementView writeListElementBytes(const char* bytes, size_t byteSize);

    MapView beginMap(size_t entryCount, size_t byteSize);
    ListElementView beginNestedMap(size_t entryCount, size_t byteSize);
    void writeMapKey(std::string_view key);
    void writeMapValue(std::string_view value);
    void writeMapValue(std::span<const float> value);

    template <typename T>
    void writeMapValue(const T& value) {
        const db::ListBufferTypeTag tag = db::TypeToListBufferTag<T>::Tag;
        appendBytes(&tag, sizeof(tag));
        appendBytes(&value, sizeof(value));

        countElementWritten();
    }

    void writeMapValueBytes(const char* bytes, size_t byteSize);

    bool topMapExpectsValue() const;

    bool hasOpenContainer() const;
    bool topContainerIsMap() const;
    bool topContainerComplete() const;
    void popContainer();
    size_t openContainerCount() const;
    size_t topLevelValuesWritten() const;

    std::span<const char> getNestedBytes();
    std::span<const std::string_view> getNestedStrings() const;

    void reset();

private:
    struct OpenContainer {
        uint32_t _expectedCount {0};
        uint32_t _writtenCount {0};
        bool _isMap {false};
        bool _keyPending {false};
    };

    // The decoder hands over an embedding view before filling its floats (the payload
    // may still be in flight across packets), so the copy into the reserved list slot
    // waits until the list bytes are read.
    struct DeferredPayload {
        uint32_t _destinationOffset {0};
        const char* _source {nullptr};
        uint32_t _byteSize {0};
    };

    net::proto::ChunkedBuffer<float> _embeddingBuffer;
    net::proto::ChunkedBuffer<char> _stringBuffer;
    std::vector<char> _nestedBytes;
    std::vector<std::string_view> _nestedStrings;
    std::vector<OpenContainer> _containerStack;
    std::vector<DeferredPayload> _deferredPayloads;

    uint32_t appendBytes(const void* bytes, size_t byteSize);
    uint32_t appendCountHeader(size_t count);
    ListElementView appendNestedHeader(db::ListBufferTypeTag tag, size_t count);
    ListElementView appendDeferredPayload(db::ListBufferTypeTag tag, const char* source, size_t byteSize);
    void countElementWritten();
};

}
