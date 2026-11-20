// Copyright Turing Biosystems Ltd
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <span>
#include <string_view>
#include <vector>

#include "ChunkedBuffer.h"
#include "CellTag.h"
#include "CellUtils.h"
#include "PathView.h"

#include "WasmCellView.h"
#include "ListView.h"

namespace wasm {

// Lists and maps are serialised depth-first into one flat little-endian byte buffer that JS
// reads directly: a list is [u32 elementCount] then its elements, an element [u8 tag] then
// its payload — 8 bytes for Int/UInt/Double/NodeID/EdgeID, 1 for Bool/Null, [u32 index] into
// the nested string set for String, [u32 byteSize][bytes] for Embedding, a nested list or map inline
// for ListView or MapView. A map is [u32 entryCount] then its entries, an entry [u32 index]
// of its key in the nested string set then its value, written exactly as a list element is.
class WasmValueStore {
public:
    using CellView = WasmCellView;

    WasmValueStore();
    ~WasmValueStore();

    float* allocEmbedding(size_t numFloats);
    std::span<const float> getEmbeddingView(float* data, size_t numFloats);

    uint64_t* allocPathIDs(size_t idCount);
    db::PathView getPathView(uint64_t* ids, size_t idCount);

    char* allocString(size_t size);
    std::string_view getStringView(char* data, size_t size);

    ListView beginList(size_t elementCount, size_t byteSize);
    CellView beginNestedList(size_t elementCount, size_t byteSize);
    CellView writeListValue(std::string_view value);
    CellView writeListValue(std::span<const float> value);
    CellView writeListValue(db::PathView value);
    CellView writeCellBytes(const char* bytes, size_t byteSize);

    MapView beginMap(size_t entryCount, size_t byteSize);
    CellView beginNestedMap(size_t entryCount, size_t byteSize);
    void writeMapKey(std::string_view key);
    void writeMapValue(std::string_view value);
    void writeMapValue(std::span<const float> value);
    void writeMapValue(db::PathView value);

    template <typename T>
    void writeMapValue(const T& value) {
        const db::CellTag tag = db::TypeToCellTag<T>::Tag;
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

    // The decoder hands over an embedding or a path view before filling its payload (which
    // may still be in flight across packets), so the copy into the reserved list slot
    // waits until the list bytes are read.
    struct DeferredPayload {
        uint32_t _destinationOffset {0};
        const char* _source {nullptr};
        uint32_t _byteSize {0};
    };

    net::proto::ChunkedBuffer<float> _embeddingBuffer;
    net::proto::ChunkedBuffer<uint64_t> _pathBuffer;
    net::proto::ChunkedBuffer<char> _stringBuffer;
    std::vector<char> _nestedBytes;
    std::vector<std::string_view> _nestedStrings;
    std::vector<OpenContainer> _containerStack;
    std::vector<DeferredPayload> _deferredPayloads;

    uint32_t appendBytes(const void* bytes, size_t byteSize);
    uint32_t appendCountHeader(size_t count);
    CellView appendNestedHeader(db::CellTag tag, size_t count);
    CellView appendDeferredPayload(db::CellTag tag, const char* source, size_t byteSize);
    void countElementWritten();
};

}
