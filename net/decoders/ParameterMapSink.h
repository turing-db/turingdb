#pragma once

#include <optional>

#include "ParameterMapValueStore.h"
#include "TuringSinkColumnContainer.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "CellView.h"
#include "list/ListView.h"
#include "map/MapView.h"

#include "BioAssert.h"

namespace db {
class ParameterMap;
}

namespace net::proto {

class ParameterMapSink {
public:
    // Only the map decoder runs on this sink: the column family satisfies the concept.
    using ColumnContainer = TuringSinkColumnContainer;
    using ValueStore = ParameterMapValueStore;

    using Column = db::Column;

    template <typename T>
    using ColumnVector = db::ColumnVector<T>;

    template <typename T>
    using ColumnOptVector = db::ColumnOptVector<T>;

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
    using CellView = db::CellView;
    using MapView = db::MapView;

    explicit ParameterMapSink(db::ParameterMap* parameters);
    ~ParameterMapSink();

    template <typename T>
    Column* alloc();

    ParameterMapValueStore* getValues() { return &_values; }

private:
    ParameterMapValueStore _values;
};

template <typename T>
ParameterMapSink::Column* ParameterMapSink::alloc() {
    bioassert(false, "A parameter map sink allocates no column");
    return nullptr;
}

}
