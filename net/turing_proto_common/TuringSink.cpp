#include "TuringSink.h"

#include "ChunkedBuffer.h"
#include "list/ListUtils.h"
#include "metadata/PropertyType.h"

using namespace net::proto;

TuringSink::TuringSink(db::LocalMemory* localMemory,
                       ChunkedBuffer<float>* embeddingBuffer,
                       ChunkedBuffer<char>* stringBuffer,
                       db::ListBuffer<>* listBuffer)
    : _localMemory(localMemory),
    _embeddingBuffer(embeddingBuffer),
    _stringBuffer(stringBuffer),
    _listBuffer(listBuffer)
{
}

TuringSink::~TuringSink() {
}

float* TuringSink::allocEmbedding(size_t numFloats) {
    return _embeddingBuffer->alloc(numFloats);
}

std::span<const float> TuringSink::getEmbeddingView(float* data, size_t numFloats) {
    return _embeddingBuffer->getView(data, numFloats);
}

char* TuringSink::allocString(size_t size) {
    return _stringBuffer->alloc(size);
}

std::string_view TuringSink::getStringView(char* data, size_t size) {
    return _stringBuffer->getView(data, size);
}

db::ListView TuringSink::beginList(size_t elementCount, size_t byteSize) {
    _listStack.push_back(_listBuffer->reserveList(elementCount, byteSize));

    return _listStack.back().getView();
}

db::ListElementView TuringSink::beginNestedList(size_t elementCount, size_t byteSize) {
    const db::ListWriteCursor childCursor = _listBuffer->reserveList(elementCount, byteSize);

    // The parent's nested slot stores the child's ListView; the child's elements then
    // stream through the cursor pushed on top.
    const db::ListElementView elementView =
        _listStack.back().writeValue<db::ListView>(db::TypeToListBufferTag<db::ListView>::Tag, childCursor.getView());
    _listStack.push_back(childCursor);

    return elementView;
}

db::ListElementView TuringSink::writeListValue(std::string_view value) {
    return _listStack.back().writeValue<db::types::String::Primitive>(db::TypeToListBufferTag<db::types::String::Primitive>::Tag, value);
}

db::ListElementView TuringSink::writeListValue(std::span<const float> value) {
    return _listStack.back().writeValue<db::types::Embedding::Primitive>(db::TypeToListBufferTag<db::types::Embedding::Primitive>::Tag, value);
}

db::ListElementView TuringSink::writeListElementBytes(const char* bytes, size_t byteSize) {
    return _listStack.back().writeRaw(bytes, byteSize);
}

void TuringSink::reset() {
    _listStack.clear();
}
