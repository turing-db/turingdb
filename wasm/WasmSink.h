#pragma once

#include <stddef.h>
#include <deque>
#include <optional>
#include <span>
#include <string_view>

#include "ChunkedBuffer.h"
#include "TuringProtoDecoderConcepts.h"
#include "list/ListBuffer.h"

#include "Column.h"
#include "ColumnConst.h"
#include "ColumnContainer.h"
#include "ColumnVector.h"

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
    } else if constexpr (std::is_same_v<Element, db::Path>) {
        return ColumnType::PATH;
    } else if constexpr (std::is_same_v<Element, db::EntityList>) {
        return ColumnType::ENTITY_LIST;
    } else if constexpr (std::is_same_v<Element, db::ListView>) {
        return ColumnType::LIST_VIEW;
    } else if constexpr (std::is_same_v<Element, db::ListElementView>) {
        return ColumnType::LIST_ELEMENT_VIEW;
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

// The wasm decode family: columns own their values outright (no arena views except
// embeddings and strings, which live in the sink's chunked buffers, and lists, whose
// elements live in the sink's list buffer).
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

    // List handle types of this family: the decoder writes lists through these. The
    // db list machinery (storage/list) is self-contained, so the wasm family reuses
    // it rather than mirroring it.
    using ListView = db::ListView;
    using ListElementView = db::ListElementView;

    WasmSink();
    ~WasmSink();

    template <typename ColumnT>
    Column* alloc() { return new ColumnT(wireCodeOfElement<typename ColumnT::ValueType>()); }

    float* allocEmbedding(size_t numFloats);
    std::span<const float> getEmbeddingView(float* data, size_t numFloats);

    char* allocString(size_t size);
    std::string_view getStringView(char* data, size_t size);

    // List builder surface: elements land as tagged bytes in the list buffer behind a
    // stack of write cursors, one per open nesting level (resumable across chunks).
    ListView beginList(size_t elementCount, size_t byteSize);
    ListElementView beginNestedList(size_t elementCount, size_t byteSize);
    ListElementView writeListValue(std::string_view value);
    ListElementView writeListValue(std::span<const float> value);
    ListElementView writeListElementBytes(const char* bytes, size_t byteSize);
    bool hasOpenList() const;
    bool topListComplete() const;
    void popList();
    size_t openListCount() const;
    size_t topLevelElementsWritten() const;

    void reset();

private:
    net::proto::ChunkedBuffer<float> _embeddingBuffer;
    net::proto::ChunkedBuffer<char> _stringBuffer;
    db::ListBuffer<> _listBuffer;

    // Front is the column's own (top-level) cursor; back is the innermost open list.
    std::deque<db::ListWriteCursor> _listStack;
};

}
