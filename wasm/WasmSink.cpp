#include "WasmSink.h"

#include <string.h>

#include "ID.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

using namespace wasm;

// The fixed-width list payload sizes the JS reader assumes.
static_assert(sizeof(db::ListBufferTypeTag) == 1);
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
    const ListView list {appendListHeader(elementCount)};
    _listStack.push_back({static_cast<uint32_t>(elementCount), 0});

    return list;
}

ListElementView WasmSink::beginNestedList(size_t elementCount, size_t byteSize) {
    const db::ListBufferTypeTag tag = db::ListBufferTypeTag::ListView;
    const ListElementView element {appendBytes(&tag, sizeof(tag))};
    appendListHeader(elementCount);

    countElementWritten();
    _listStack.push_back({static_cast<uint32_t>(elementCount), 0});

    return element;
}

ListElementView WasmSink::writeListValue(std::string_view value) {
    const db::ListBufferTypeTag tag = db::ListBufferTypeTag::String;
    const ListElementView element {appendBytes(&tag, sizeof(tag))};
    const uint32_t stringIndex = static_cast<uint32_t>(_listStrings.size());
    appendBytes(&stringIndex, sizeof(stringIndex));
    _listStrings.push_back(value);

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

bool WasmSink::hasOpenList() const {
    return !_listStack.empty();
}

bool WasmSink::topListComplete() const {
    const OpenList& top = _listStack.back();
    return top._writtenCount == top._expectedCount;
}

void WasmSink::popList() {
    _listStack.pop_back();
}

size_t WasmSink::openListCount() const {
    return _listStack.size();
}

size_t WasmSink::topLevelElementsWritten() const {
    return _listStack.front()._writtenCount;
}

std::span<const char> WasmSink::getListBytes() {
    for (const DeferredPayload& payload : _deferredPayloads) {
        memcpy(_listBytes.data() + payload._destinationOffset, payload._source, payload._byteSize);
    }
    _deferredPayloads.clear();

    return _listBytes;
}

std::span<const std::string_view> WasmSink::getListStrings() const {
    return _listStrings;
}

void WasmSink::reset() {
    _embeddingBuffer.clear();
    _stringBuffer.clear();
    _listBytes.clear();
    _listStrings.clear();
    _listStack.clear();
    _deferredPayloads.clear();
}

uint32_t WasmSink::appendBytes(const void* bytes, size_t byteSize) {
    const uint32_t offset = static_cast<uint32_t>(_listBytes.size());
    const char* first = static_cast<const char*>(bytes);
    _listBytes.insert(_listBytes.end(), first, first + byteSize);

    return offset;
}

uint32_t WasmSink::appendListHeader(size_t elementCount) {
    const uint32_t count = static_cast<uint32_t>(elementCount);
    return appendBytes(&count, sizeof(count));
}

ListElementView WasmSink::appendDeferredPayload(db::ListBufferTypeTag tag, const char* source, size_t byteSize) {
    const ListElementView element {appendBytes(&tag, sizeof(tag))};
    const uint32_t size = static_cast<uint32_t>(byteSize);
    appendBytes(&size, sizeof(size));

    const uint32_t destinationOffset = static_cast<uint32_t>(_listBytes.size());
    _listBytes.resize(_listBytes.size() + byteSize);
    _deferredPayloads.push_back({destinationOffset, source, size});

    countElementWritten();

    return element;
}

void WasmSink::countElementWritten() {
    ++_listStack.back()._writtenCount;
}
