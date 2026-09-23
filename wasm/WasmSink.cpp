#include "WasmSink.h"

#include <string.h>

#include "ID.h"
#include "TuringException.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

using namespace wasm;

namespace {

// A map value is written into the flat bytes under the list tag of the same type, so JS reads
// a map value and a list element with one decoder. Only the fixed-width values arrive as raw
// [tag][value] bytes; the others reach the sink through their own entry points.
db::ListBufferTypeTag listTagOf(db::MapBufferTypeTag tag) {
    switch (tag) {
        case db::MapBufferTypeTag::Int:
            return db::ListBufferTypeTag::Int;
        break;
        case db::MapBufferTypeTag::UInt:
            return db::ListBufferTypeTag::UInt;
        break;
        case db::MapBufferTypeTag::Double:
            return db::ListBufferTypeTag::Double;
        break;
        case db::MapBufferTypeTag::Bool:
            return db::ListBufferTypeTag::Bool;
        break;
        case db::MapBufferTypeTag::Null:
            return db::ListBufferTypeTag::Null;
        break;
        case db::MapBufferTypeTag::NodeID:
            return db::ListBufferTypeTag::NodeID;
        break;
        case db::MapBufferTypeTag::EdgeID:
            return db::ListBufferTypeTag::EdgeID;
        break;
        case db::MapBufferTypeTag::DateTime:
            return db::ListBufferTypeTag::DateTime;
        break;
        case db::MapBufferTypeTag::String:
        case db::MapBufferTypeTag::Embedding:
        case db::MapBufferTypeTag::ListView:
        case db::MapBufferTypeTag::MapView:
        case db::MapBufferTypeTag::INVALID:
        break;
    }

    throw TuringException("A map value of this type does not arrive as raw bytes");
}

}

// The fixed-width list payload sizes the JS reader assumes.
static_assert(sizeof(db::ListBufferTypeTag) == 1);
static_assert(sizeof(db::MapBufferTypeTag) == 1);
static_assert(sizeof(db::types::Int64::Primitive) == 8);
static_assert(sizeof(db::types::UInt64::Primitive) == 8);
static_assert(sizeof(db::types::Double::Primitive) == 8);
static_assert(sizeof(db::types::Bool::Primitive) == 1);
static_assert(sizeof(db::PropertyNull) == 1);
static_assert(sizeof(db::NodeID) == 8);
static_assert(sizeof(db::EdgeID) == 8);

WasmSink::WasmSink()
{
}

WasmSink::~WasmSink() {
}

float* WasmSink::allocEmbedding(size_t numFloats) {
    return _embeddingBuffer.alloc(numFloats);
}

std::span<const float> WasmSink::getEmbeddingView(float* data, size_t numFloats) {
    return _embeddingBuffer.getView(data, numFloats);
}

char* WasmSink::allocString(size_t size) {
    return _stringBuffer.alloc(size);
}

std::string_view WasmSink::getStringView(char* data, size_t size) {
    return _stringBuffer.getView(data, size);
}

ListView WasmSink::beginList(size_t elementCount, size_t byteSize) {
    const ListView list {appendCountHeader(elementCount)};
    _containerStack.push_back({._expectedCount = static_cast<uint32_t>(elementCount)});

    return list;
}

ListElementView WasmSink::beginNestedList(size_t elementCount, size_t byteSize) {
    const ListElementView element = appendNestedHeader(db::ListBufferTypeTag::ListView, elementCount);
    _containerStack.push_back({._expectedCount = static_cast<uint32_t>(elementCount)});

    return element;
}

ListElementView WasmSink::writeListValue(std::string_view value) {
    const db::ListBufferTypeTag tag = db::ListBufferTypeTag::String;
    const ListElementView element {appendBytes(&tag, sizeof(tag))};
    const uint32_t stringIndex = static_cast<uint32_t>(_nestedStrings.size());
    appendBytes(&stringIndex, sizeof(stringIndex));
    _nestedStrings.push_back(value);

    countElementWritten();

    return element;
}

ListElementView WasmSink::writeListValue(std::span<const float> value) {
    return appendDeferredPayload(db::ListBufferTypeTag::Embedding, reinterpret_cast<const char*>(value.data()), value.size_bytes());
}

ListElementView WasmSink::writeListElementBytes(const char* bytes, size_t byteSize) {
    const ListElementView element {appendBytes(bytes, byteSize)};
    countElementWritten();

    return element;
}

MapView WasmSink::beginMap(size_t entryCount, size_t byteSize) {
    const MapView map {appendCountHeader(entryCount)};
    _containerStack.push_back({._expectedCount = static_cast<uint32_t>(entryCount), ._isMap = true});

    return map;
}

ListElementView WasmSink::beginNestedMap(size_t entryCount, size_t byteSize) {
    const ListElementView element = appendNestedHeader(db::ListBufferTypeTag::MapView, entryCount);
    _containerStack.push_back({._expectedCount = static_cast<uint32_t>(entryCount), ._isMap = true});

    return element;
}

void WasmSink::writeMapKey(std::string_view key) {
    const uint32_t keyIndex = static_cast<uint32_t>(_nestedStrings.size());
    appendBytes(&keyIndex, sizeof(keyIndex));
    _nestedStrings.push_back(key);

    _containerStack.back()._keyPending = true;
}

void WasmSink::writeMapValue(std::string_view value) {
    writeListValue(value);
}

void WasmSink::writeMapValue(std::span<const float> value) {
    writeListValue(value);
}

void WasmSink::writeMapValueBytes(const char* bytes, size_t byteSize) {
    db::MapBufferTypeTag mapTag {};
    memcpy(&mapTag, bytes, sizeof(mapTag));

    const db::ListBufferTypeTag tag = listTagOf(mapTag);
    appendBytes(&tag, sizeof(tag));
    appendBytes(bytes + sizeof(mapTag), byteSize - sizeof(mapTag));

    countElementWritten();
}

bool WasmSink::topMapExpectsValue() const {
    return _containerStack.back()._keyPending;
}

bool WasmSink::hasOpenContainer() const {
    return !_containerStack.empty();
}

bool WasmSink::topContainerIsMap() const {
    return _containerStack.back()._isMap;
}

bool WasmSink::topContainerComplete() const {
    const OpenContainer& top = _containerStack.back();
    return top._writtenCount == top._expectedCount;
}

void WasmSink::popContainer() {
    _containerStack.pop_back();
}

size_t WasmSink::openContainerCount() const {
    return _containerStack.size();
}

size_t WasmSink::topLevelValuesWritten() const {
    return _containerStack.front()._writtenCount;
}

std::span<const char> WasmSink::getNestedBytes() {
    for (const DeferredPayload& payload : _deferredPayloads) {
        memcpy(_nestedBytes.data() + payload._destinationOffset, payload._source, payload._byteSize);
    }
    _deferredPayloads.clear();

    return _nestedBytes;
}

std::span<const std::string_view> WasmSink::getNestedStrings() const {
    return _nestedStrings;
}

void WasmSink::reset() {
    _embeddingBuffer.clear();
    _stringBuffer.clear();
    _nestedBytes.clear();
    _nestedStrings.clear();
    _containerStack.clear();
    _deferredPayloads.clear();
}

uint32_t WasmSink::appendBytes(const void* bytes, size_t byteSize) {
    const uint32_t offset = static_cast<uint32_t>(_nestedBytes.size());
    const char* first = static_cast<const char*>(bytes);
    _nestedBytes.insert(_nestedBytes.end(), first, first + byteSize);

    return offset;
}

uint32_t WasmSink::appendCountHeader(size_t count) {
    const uint32_t wireCount = static_cast<uint32_t>(count);
    return appendBytes(&wireCount, sizeof(wireCount));
}

ListElementView WasmSink::appendNestedHeader(db::ListBufferTypeTag tag, size_t count) {
    const ListElementView element {appendBytes(&tag, sizeof(tag))};
    appendCountHeader(count);

    countElementWritten();

    return element;
}

ListElementView WasmSink::appendDeferredPayload(db::ListBufferTypeTag tag, const char* source, size_t byteSize) {
    const ListElementView element {appendBytes(&tag, sizeof(tag))};
    const uint32_t size = static_cast<uint32_t>(byteSize);
    appendBytes(&size, sizeof(size));

    const uint32_t destinationOffset = static_cast<uint32_t>(_nestedBytes.size());
    _nestedBytes.resize(_nestedBytes.size() + byteSize);
    _deferredPayloads.push_back({destinationOffset, source, size});

    countElementWritten();

    return element;
}

void WasmSink::countElementWritten() {
    OpenContainer& top = _containerStack.back();
    ++top._writtenCount;
    top._keyPending = false;
}
