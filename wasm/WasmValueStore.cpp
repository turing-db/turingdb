// Copyright Turing Biosystems Ltd
// SPDX-License-Identifier: Apache-2.0

#include "WasmValueStore.h"

#include <string.h>

#include "ID.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

using namespace wasm;

// The fixed-width list payload sizes the JS reader assumes.
static_assert(sizeof(db::CellTag) == 1);
static_assert(sizeof(db::types::Int64::Primitive) == 8);
static_assert(sizeof(db::types::UInt64::Primitive) == 8);
static_assert(sizeof(db::types::Double::Primitive) == 8);
static_assert(sizeof(db::types::Bool::Primitive) == 1);
static_assert(sizeof(db::PropertyNull) == 1);
static_assert(sizeof(db::NodeID) == 8);
static_assert(sizeof(db::EdgeID) == 8);

WasmValueStore::WasmValueStore()
{
}

WasmValueStore::~WasmValueStore() {
}

float* WasmValueStore::allocEmbedding(size_t numFloats) {
    return _embeddingBuffer.alloc(numFloats);
}

std::span<const float> WasmValueStore::getEmbeddingView(float* data, size_t numFloats) {
    return _embeddingBuffer.getView(data, numFloats);
}

uint64_t* WasmValueStore::allocPathIDs(size_t idCount) {
    return _pathBuffer.alloc(idCount);
}

db::PathView WasmValueStore::getPathView(uint64_t* ids, size_t idCount) {
    return db::PathView {_pathBuffer.getView(ids, idCount)};
}

char* WasmValueStore::allocString(size_t size) {
    return _stringBuffer.alloc(size);
}

std::string_view WasmValueStore::getStringView(char* data, size_t size) {
    return _stringBuffer.getView(data, size);
}

ListView WasmValueStore::beginList(size_t elementCount, size_t byteSize) {
    const ListView list {appendCountHeader(elementCount)};
    _containerStack.push_back({._expectedCount = static_cast<uint32_t>(elementCount)});

    return list;
}

WasmCellView WasmValueStore::beginNestedList(size_t elementCount, size_t byteSize) {
    const CellView element = appendNestedHeader(db::CellTag::ListView, elementCount);
    _containerStack.push_back({._expectedCount = static_cast<uint32_t>(elementCount)});

    return element;
}

WasmCellView WasmValueStore::writeListValue(std::string_view value) {
    const db::CellTag tag = db::CellTag::String;
    const CellView element {appendBytes(&tag, sizeof(tag))};
    const uint32_t stringIndex = static_cast<uint32_t>(_nestedStrings.size());
    appendBytes(&stringIndex, sizeof(stringIndex));
    _nestedStrings.push_back(value);

    countElementWritten();

    return element;
}

WasmCellView WasmValueStore::writeListValue(std::span<const float> value) {
    return appendDeferredPayload(db::CellTag::Embedding, reinterpret_cast<const char*>(value.data()), value.size_bytes());
}

WasmCellView WasmValueStore::writeListValue(db::PathView value) {
    const std::span<const uint64_t> ids = value.getIDs();
    return appendDeferredPayload(db::CellTag::Path, reinterpret_cast<const char*>(ids.data()), ids.size_bytes());
}

WasmCellView WasmValueStore::writeCellBytes(const char* bytes, size_t byteSize) {
    const CellView element {appendBytes(bytes, byteSize)};
    countElementWritten();

    return element;
}

MapView WasmValueStore::beginMap(size_t entryCount, size_t byteSize) {
    const MapView map {appendCountHeader(entryCount)};
    _containerStack.push_back({._expectedCount = static_cast<uint32_t>(entryCount), ._isMap = true});

    return map;
}

WasmCellView WasmValueStore::beginNestedMap(size_t entryCount, size_t byteSize) {
    const CellView element = appendNestedHeader(db::CellTag::MapView, entryCount);
    _containerStack.push_back({._expectedCount = static_cast<uint32_t>(entryCount), ._isMap = true});

    return element;
}

void WasmValueStore::writeMapKey(std::string_view key) {
    const uint32_t keyIndex = static_cast<uint32_t>(_nestedStrings.size());
    appendBytes(&keyIndex, sizeof(keyIndex));
    _nestedStrings.push_back(key);

    _containerStack.back()._keyPending = true;
}

void WasmValueStore::writeMapValue(std::string_view value) {
    writeListValue(value);
}

void WasmValueStore::writeMapValue(std::span<const float> value) {
    writeListValue(value);
}

void WasmValueStore::writeMapValue(db::PathView value) {
    writeListValue(value);
}

void WasmValueStore::writeMapValueBytes(const char* bytes, size_t byteSize) {
    appendBytes(bytes, byteSize);

    countElementWritten();
}

bool WasmValueStore::topMapExpectsValue() const {
    return _containerStack.back()._keyPending;
}

bool WasmValueStore::hasOpenContainer() const {
    return !_containerStack.empty();
}

bool WasmValueStore::topContainerIsMap() const {
    return _containerStack.back()._isMap;
}

bool WasmValueStore::topContainerComplete() const {
    const OpenContainer& top = _containerStack.back();
    return top._writtenCount == top._expectedCount;
}

void WasmValueStore::popContainer() {
    _containerStack.pop_back();
}

size_t WasmValueStore::openContainerCount() const {
    return _containerStack.size();
}

size_t WasmValueStore::topLevelValuesWritten() const {
    return _containerStack.front()._writtenCount;
}

std::span<const char> WasmValueStore::getNestedBytes() {
    for (const DeferredPayload& payload : _deferredPayloads) {
        memcpy(_nestedBytes.data() + payload._destinationOffset, payload._source, payload._byteSize);
    }
    _deferredPayloads.clear();

    return _nestedBytes;
}

std::span<const std::string_view> WasmValueStore::getNestedStrings() const {
    return _nestedStrings;
}

void WasmValueStore::reset() {
    _embeddingBuffer.clear();
    _pathBuffer.clear();
    _stringBuffer.clear();
    _nestedBytes.clear();
    _nestedStrings.clear();
    _containerStack.clear();
    _deferredPayloads.clear();
}

uint32_t WasmValueStore::appendBytes(const void* bytes, size_t byteSize) {
    const uint32_t offset = static_cast<uint32_t>(_nestedBytes.size());
    const char* first = static_cast<const char*>(bytes);
    _nestedBytes.insert(_nestedBytes.end(), first, first + byteSize);

    return offset;
}

uint32_t WasmValueStore::appendCountHeader(size_t count) {
    const uint32_t wireCount = static_cast<uint32_t>(count);
    return appendBytes(&wireCount, sizeof(wireCount));
}

WasmCellView WasmValueStore::appendNestedHeader(db::CellTag tag, size_t count) {
    const CellView element {appendBytes(&tag, sizeof(tag))};
    appendCountHeader(count);

    countElementWritten();

    return element;
}

WasmCellView WasmValueStore::appendDeferredPayload(db::CellTag tag, const char* source, size_t byteSize) {
    const CellView element {appendBytes(&tag, sizeof(tag))};
    const uint32_t size = static_cast<uint32_t>(byteSize);
    appendBytes(&size, sizeof(size));

    const uint32_t destinationOffset = static_cast<uint32_t>(_nestedBytes.size());
    _nestedBytes.resize(_nestedBytes.size() + byteSize);
    _deferredPayloads.push_back({destinationOffset, source, size});

    countElementWritten();

    return element;
}

void WasmValueStore::countElementWritten() {
    OpenContainer& top = _containerStack.back();
    ++top._writtenCount;
    top._keyPending = false;
}
