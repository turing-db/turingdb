#include "TuringSink.h"

#include "FatalException.h"
#include "list/ListUtils.h"

using namespace net::proto;

TuringSink::TuringSink(db::LocalMemory* localMemory,
                       ChunkedBuffer<float>* embeddingBuffer,
                       ChunkedBuffer<char>* stringBuffer,
                       db::ListBuffer<>* listBuffer,
                       db::MapBuffer<>* mapBuffer)
    : _localMemory(localMemory),
    _embeddingBuffer(embeddingBuffer),
    _stringBuffer(stringBuffer),
    _listBuffer(listBuffer),
    _mapBuffer(mapBuffer)
{
}

TuringSink::~TuringSink() {
}

db::ListView TuringSink::beginList(size_t elementCount, size_t byteSize) {
    _containerStack.push_back(NestedContainerCursor::list(_listBuffer->reserveList(elementCount, byteSize)));

    return listCursor().getView();
}

db::ListElementView TuringSink::beginNestedList(size_t elementCount, size_t byteSize) {
    const db::ListWriteCursor childCursor = _listBuffer->reserveList(elementCount, byteSize);
    const db::ListView childView = childCursor.getView();

    db::ListElementView elementView;

    if (topContainerIsMap()) {
        mapCursor().writeValue<db::ListView>(db::TypeToMapBufferTag<db::ListView>::Tag, childView);
    } else {
        elementView = listCursor().writeValue<db::ListView>(db::TypeToListBufferTag<db::ListView>::Tag, childView);
    }

    _containerStack.push_back(NestedContainerCursor::list(childCursor));

    return elementView;
}

db::MapView TuringSink::beginMap(size_t entryCount, size_t byteSize) {
    _containerStack.push_back(NestedContainerCursor::map(_mapBuffer->reserveMap(entryCount, byteSize)));

    return mapCursor().getView();
}

db::ListElementView TuringSink::beginNestedMap(size_t entryCount, size_t byteSize) {
    const db::MapWriteCursor childCursor = _mapBuffer->reserveMap(entryCount, byteSize);
    const db::MapView childView = childCursor.getView();

    db::ListElementView elementView;
    if (topContainerIsMap()) {
        mapCursor().writeValue<db::MapView>(db::TypeToMapBufferTag<db::MapView>::Tag, childView);
    } else {
        elementView = listCursor().writeValue<db::MapView>(db::TypeToListBufferTag<db::MapView>::Tag, childView);
    }

    _containerStack.push_back(NestedContainerCursor::map(childCursor));

    return elementView;
}

bool TuringSink::topContainerComplete() const {
    return _containerStack.back().isComplete();
}

void TuringSink::reset() {
    _containerStack.clear();
}
