#include "TuringValueStore.h"

#include "BioAssert.h"

using namespace net::proto;

TuringValueStore::TuringValueStore()
{
}

TuringValueStore::~TuringValueStore() {
}

char* TuringValueStore::allocString(size_t size) {
    return _stringBuffer.allocate(size).data();
}

db::ListView TuringValueStore::beginList(size_t elementCount, size_t byteSize) {
    _containerStack.push_back(NestedContainerCursor::list(_listBuffer.reserveList(elementCount, byteSize)));

    return listCursor().getView();
}

db::CellView TuringValueStore::beginNestedList(size_t elementCount, size_t byteSize) {
    const db::ListWriteCursor childCursor = _listBuffer.reserveList(elementCount, byteSize);
    const db::ListView childView = childCursor.getView();

    db::CellView elementView;

    if (topContainerIsMap()) {
        mapCursor().writeValue<db::ListView>(db::TypeToCellTag<db::ListView>::Tag, childView);
    } else {
        elementView = listCursor().writeValue<db::ListView>(db::TypeToCellTag<db::ListView>::Tag, childView);
    }

    _containerStack.push_back(NestedContainerCursor::list(childCursor));

    return elementView;
}

db::MapView TuringValueStore::beginMap(size_t entryCount, size_t byteSize) {
    _containerStack.push_back(NestedContainerCursor::map(_mapBuffer.reserveMap(entryCount, byteSize)));

    return mapCursor().getView();
}

db::CellView TuringValueStore::beginNestedMap(size_t entryCount, size_t byteSize) {
    const db::MapWriteCursor childCursor = _mapBuffer.reserveMap(entryCount, byteSize);
    const db::MapView childView = childCursor.getView();

    db::CellView elementView;
    if (topContainerIsMap()) {
        mapCursor().writeValue<db::MapView>(db::TypeToCellTag<db::MapView>::Tag, childView);
    } else {
        elementView = listCursor().writeValue<db::MapView>(db::TypeToCellTag<db::MapView>::Tag, childView);
    }

    _containerStack.push_back(NestedContainerCursor::map(childCursor));

    return elementView;
}

bool TuringValueStore::topContainerComplete() const {
    return _containerStack.back().isComplete();
}

void TuringValueStore::reset() {
    _containerStack.clear();
}

void TuringValueStore::clearValues() {
    bioassert(_containerStack.empty(), "Values cleared while a container is still open");

    _stringBuffer.clear();
    _embeddingBuffer.clear();
    _pathBuffer.clear();
    _listBuffer.clear();
    _mapBuffer.clear();
}
