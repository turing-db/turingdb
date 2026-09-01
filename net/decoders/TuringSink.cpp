#include "TuringSink.h"

#include "list/ListUtils.h"

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

db::ListView TuringSink::beginList(size_t elementCount, size_t byteSize) {
    _listStack.push_back(_listBuffer->reserveList(elementCount, byteSize));

    return _listStack.back().getView();
}

db::ListElementView TuringSink::beginNestedList(size_t elementCount, size_t byteSize) {
    const db::ListWriteCursor childCursor = _listBuffer->reserveList(elementCount, byteSize);

    const db::ListElementView elementView =
        _listStack.back().writeValue<db::ListView>(db::TypeToListBufferTag<db::ListView>::Tag, childCursor.getView());
    _listStack.push_back(childCursor);

    return elementView;
}

void TuringSink::reset() {
    _listStack.clear();
}
