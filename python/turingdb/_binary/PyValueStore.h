#pragma once

#include <stddef.h>
#include <stdint.h>
#include <span>
#include <string_view>

#include "TuringValueStore.h"
#include "CellView.h"
#include "PathView.h"
#include "list/ListView.h"
#include "map/MapView.h"

namespace pybindings {

// The value store of the Python sink. Its values are written to a TuringValueStore and
// dropped with clearValues.
class PyValueStore {
public:
    PyValueStore();
    ~PyValueStore();

    char* allocString(size_t size) { return _values.allocString(size); }
    static std::string_view getStringView(char* data, size_t size) { return net::proto::TuringValueStore::getStringView(data, size); }

    float* allocEmbedding(size_t floatCount) { return _values.allocEmbedding(floatCount); }
    std::span<const float> getEmbeddingView(float* data, size_t floatCount) { return _values.getEmbeddingView(data, floatCount); }

    uint64_t* allocPathIDs(size_t idCount) { return _values.allocPathIDs(idCount); }
    db::PathView getPathView(uint64_t* ids, size_t idCount) { return _values.getPathView(ids, idCount); }

    db::ListView beginList(size_t elementCount, size_t byteSize) { return _values.beginList(elementCount, byteSize); }
    db::CellView beginNestedList(size_t elementCount, size_t byteSize) { return _values.beginNestedList(elementCount, byteSize); }
    db::CellView writeListValue(std::string_view value) { return _values.writeListValue(value); }
    db::CellView writeListValue(std::span<const float> value) { return _values.writeListValue(value); }
    db::CellView writeListValue(db::PathView value) { return _values.writeListValue(value); }
    db::CellView writeCellBytes(const char* bytes, size_t byteSize) { return _values.writeCellBytes(bytes, byteSize); }

    db::MapView beginMap(size_t entryCount, size_t byteSize) { return _values.beginMap(entryCount, byteSize); }
    db::CellView beginNestedMap(size_t entryCount, size_t byteSize) { return _values.beginNestedMap(entryCount, byteSize); }
    void writeMapKey(std::string_view key) { _values.writeMapKey(key); }

    template <typename T>
    void writeMapValue(const T& value) { _values.writeMapValue(value); }

    void writeMapValueBytes(const char* bytes, size_t byteSize) { _values.writeMapValueBytes(bytes, byteSize); }
    bool topMapExpectsValue() const { return _values.topMapExpectsValue(); }

    bool hasOpenContainer() const { return _values.hasOpenContainer(); }
    bool topContainerIsMap() const { return _values.topContainerIsMap(); }
    bool topContainerComplete() const { return _values.topContainerComplete(); }
    void popContainer() { _values.popContainer(); }
    size_t openContainerCount() const { return _values.openContainerCount(); }
    size_t topLevelValuesWritten() const { return _values.topLevelValuesWritten(); }

    void reset() { _values.reset(); }
    void clearValues() { _values.clearValues(); }

private:
    net::proto::TuringValueStore _values;
};

}
