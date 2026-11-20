#pragma once

#include <stddef.h>
#include <stdint.h>
#include <span>
#include <string_view>

#include "ChunkedBuffer.h"
#include "NestedContainerCursor.h"
#include "CellUtils.h"
#include "buffers/StringBuffer.h"
#include "list/ListBuffer.h"
#include "map/MapBuffer.h"
#include "map/MapView.h"
#include "metadata/PropertyType.h"

namespace net::proto {

// The values of a sink whose values are the engine's own: strings, embeddings and paths in
// their buffers, lists and maps built in a ListBuffer and a MapBuffer.
class TuringValueStore {
public:
    TuringValueStore();
    ~TuringValueStore();

    float* allocEmbedding(size_t numFloats) { return _embeddingBuffer.alloc(numFloats); }
    std::span<const float> getEmbeddingView(float* data, size_t numFloats) { return _embeddingBuffer.getView(data, numFloats); }

    uint64_t* allocPathIDs(size_t idCount) { return _pathBuffer.alloc(idCount); }
    db::PathView getPathView(uint64_t* ids, size_t idCount) { return db::PathView {_pathBuffer.getView(ids, idCount)}; }

    char* allocString(size_t size);
    static std::string_view getStringView(char* data, size_t size) { return {data, size}; }

    // Container builder surface: the decoder reports list and map structure as it comes off
    // the wire. Both kinds are reserved up front and filled through a write cursor, so a
    // container's view is valid from the moment it opens and the parent can be given it
    // before any of its contents land.
    db::ListView beginList(size_t elementCount, size_t byteSize);
    db::CellView beginNestedList(size_t elementCount, size_t byteSize);

    db::CellView writeListValue(std::string_view value) {
        return listCursor().writeValue<db::types::String::Primitive>(db::TypeToCellTag<db::types::String::Primitive>::Tag, value);
    }

    db::CellView writeListValue(std::span<const float> value) {
        return listCursor().writeValue<db::types::Embedding::Primitive>(db::TypeToCellTag<db::types::Embedding::Primitive>::Tag, value);
    }

    db::CellView writeListValue(db::PathView value) {
        return listCursor().writeValue<db::PathView>(db::TypeToCellTag<db::PathView>::Tag, value);
    }

    db::CellView writeCellBytes(const char* bytes, size_t byteSize) {
        return listCursor().writeRaw(bytes, byteSize);
    }

    db::MapView beginMap(size_t entryCount, size_t byteSize);
    db::CellView beginNestedMap(size_t entryCount, size_t byteSize);

    void writeMapKey(std::string_view key) { mapCursor().writeKey(key); }

    template <typename T>
    void writeMapValue(const T& value) {
        mapCursor().writeValue<T>(db::TypeToCellTag<T>::Tag, value);
    }

    void writeMapValueBytes(const char* bytes, size_t byteSize) {
        mapCursor().writeValueBytes(bytes, byteSize);
    }

    bool topMapExpectsValue() const { return mapCursor().expectsValue(); }

    bool hasOpenContainer() const { return !_containerStack.empty(); }
    bool topContainerIsMap() const { return _containerStack.back().isMap(); }
    bool topContainerComplete() const;
    void popContainer() { _containerStack.pop_back(); }
    size_t openContainerCount() const { return _containerStack.size(); }
    size_t topLevelValuesWritten() const { return _containerStack.front().getWritten(); }

    void reset();

    // Drops every value written so far. No container may be open.
    void clearValues();

private:
    db::StringBuffer _stringBuffer;
    ChunkedBuffer<float> _embeddingBuffer;
    ChunkedBuffer<uint64_t> _pathBuffer;
    db::QueryListBuffer _listBuffer;
    db::MapBuffer<> _mapBuffer;

    // Front is the column's own (top-level) container; back is the innermost open one.
    NestedContainerCursor::Stack _containerStack;

    db::ListWriteCursor& listCursor() { return _containerStack.back().getList(); }
    db::MapWriteCursor& mapCursor() { return _containerStack.back().getMap(); }
    const db::MapWriteCursor& mapCursor() const { return _containerStack.back().getMap(); }
};

}
