#pragma once

#include <optional>

#include "CellView.h"
#include "LocalMemory.h"
#include "TuringSinkColumnContainer.h"
#include "TuringValueStore.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "list/ListView.h"
#include "map/MapView.h"

namespace net::proto {

class TuringSink {
public:
    using ColumnContainer = TuringSinkColumnContainer;
    using ValueStore = TuringValueStore;

    using Column = db::Column;

    template <typename T>
    using ColumnVector = db::ColumnVector<T>;

    template <typename T>
    using ColumnOptVector = db::ColumnOptVector<T>;

    template <typename T>
    using ColumnConst = db::ColumnConst<T>;

    template <typename T>
    using ColumnOptConst = db::ColumnConst<std::optional<T>>;

    using ListView = db::ListView;
    using CellView = db::CellView;
    using MapView = db::MapView;

    TuringSink(db::LocalMemory* localMemory, TuringValueStore* values);
    ~TuringSink();

    template <typename T>
    Column* alloc() { return _localMemory->alloc<T>(); }

    // The server allocates its results from the same pools, so this sink holds every column
    // the server can send
    template <typename ColumnT>
    static consteval bool holds() { return db::LocalMemory::MemoryPools::contains<ColumnT>(); }

    TuringValueStore* getValues() { return _values; }

private:
    db::LocalMemory* _localMemory {nullptr};
    TuringValueStore* _values {nullptr};
};
}
