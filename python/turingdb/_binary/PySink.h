#pragma once

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <optional>
#include <vector>

#include "CellView.h"
#include "LocalMemory.h"
#include "PyColumn.h"
#include "PyColumnContainer.h"
#include "PyValueStore.h"
#include "list/ListView.h"
#include "map/MapView.h"

namespace pybindings {

// The decode sink of the Python client. A fixed-width column grows across the chunks of a
// response, to be handed to numpy as it is. Strings, embeddings, paths, lists and maps are
// written into the value store a chunk at a time, read into Python objects once the chunk
// is decoded, and dropped with clearValues. The path IDs are kept, as the GraphPath objects
// view them.
class PySink {
public:
    using ColumnContainer = PyColumnContainer;
    using ValueStore = PyValueStore;
    using Column = PyColumn;

    template <typename T>
    using ColumnVector = PyColumnVector<T>;

    template <typename T>
    using ColumnOptVector = PyColumnOptVector<T>;

    template <typename T>
    using ColumnConst = PyColumnConst<T>;

    template <typename T>
    using ColumnOptConst = PyColumnConst<std::optional<T>>;

    using ListView = db::ListView;
    using CellView = db::CellView;
    using MapView = db::MapView;

    explicit PySink(PyValueStore* values);
    ~PySink();

    template <typename ColumnT>
    Column* alloc() {
        _columns.push_back(std::make_unique<ColumnT>());
        return _columns.back().get();
    }

    // The server sends only the columns its pools hold, as TuringSink says
    template <typename ColumnT>
    static consteval bool holds() { return db::LocalMemory::MemoryPools::contains<typename ColumnT::DbColumn>(); }

    PyValueStore* getValues() { return _values; }

private:
    PyValueStore* _values {nullptr};
    std::vector<std::unique_ptr<PyColumn>> _columns;
};

}
