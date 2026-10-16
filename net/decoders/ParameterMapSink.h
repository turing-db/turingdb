#pragma once

#include <stddef.h>
#include <deque>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>

#include "ParameterValue.h"
#include "TuringSinkColumnContainer.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"

#include "BioAssert.h"

namespace db {
class ParameterMap;
}

namespace net::proto {

// Builds a ParameterMap from one map value of the nested container format. The map is filled
// in place: a key waits in its frame until its value arrives, and a string's bytes are read
// straight into the ParameterValue that holds it.
class ParameterMapSink {
public:
    // Only the map decoder runs on this sink: the column family satisfies the concept.
    using ColumnContainer = TuringSinkColumnContainer;

    using Column = db::Column;

    template <typename T>
    using ColumnVector = db::ColumnVector<T>;

    template <typename T>
    using ColumnOptVector = db::ColumnVector<std::optional<T>>;

    // The map decoder hands the root map to a constant column; the sink has already filled
    // the ParameterMap, so the root column keeps nothing.
    template <typename T>
    struct RootColumn {
        void set(const T& value) {}
    };

    template <typename T>
    using ColumnConst = RootColumn<T>;

    template <typename T>
    using ColumnOptConst = db::ColumnConst<std::optional<T>>;

    using ListView = db::ListView;
    using ListElementView = db::ListElementView;
    using MapView = db::MapView;
    using MapEntryView = db::MapEntryView;

    explicit ParameterMapSink(db::ParameterMap* parameters);
    ~ParameterMapSink();

    float* allocEmbedding(size_t numFloats);
    static std::span<const float> getEmbeddingView(float* data, size_t numFloats) { return {data, numFloats}; }

    char* allocString(size_t size);
    static std::string_view getStringView(char* data, size_t size) { return {data, size}; }

    template <typename T>
    Column* alloc();

    db::ListView beginList(size_t elementCount, size_t byteSize);
    db::ListElementView beginNestedList(size_t elementCount, size_t byteSize);

    db::ListElementView writeListValue(std::string_view value);
    db::ListElementView writeListValue(std::span<const float> value);
    db::ListElementView writeListElementBytes(const char* bytes, size_t byteSize);

    db::MapView beginMap(size_t entryCount, size_t byteSize);
    db::ListElementView beginNestedMap(size_t entryCount, size_t byteSize);

    void writeMapKey(std::string_view key);

    template <typename T>
    void writeMapValue(const T& value);

    void writeMapValueBytes(const char* bytes, size_t byteSize);

    db::MapEntryView lastMapEntry() const { return {}; }

    bool topMapExpectsValue() const { return _frames.back()._keyPending; }

    bool hasOpenContainer() const { return !_frames.empty(); }
    bool topContainerIsMap() const;
    bool topContainerComplete() const;
    void popContainer() { _frames.pop_back(); }
    size_t openContainerCount() const { return _frames.size(); }
    size_t topLevelValuesWritten() const { return _frames.front()._written; }

    void reset() { _frames.clear(); }

private:
    struct ParameterFrame {
        std::variant<db::ParameterMap*, db::ParameterValue::Map*, db::ParameterValue::List*> _container;
        size_t _expected {0};
        size_t _written {0};
        std::string _pendingKey;
        bool _keyPending {false};
    };

    db::ParameterMap* _parameters {nullptr};

    // A deque never moves its frames when one is pushed, so a key whose bytes are still being
    // read into _pendingKey stays where the decoder was told to write it.
    std::deque<ParameterFrame> _frames;

    db::ParameterValue& nextSlot();
    void completeValue();
};

template <typename T>
ParameterMapSink::Column* ParameterMapSink::alloc() {
    bioassert(false, "A parameter map sink allocates no column");
    return nullptr;
}

template <typename T>
void ParameterMapSink::writeMapValue(const T& value) {
    completeValue();
}

}
