#pragma once

#include <stddef.h>

#include <concepts>
#include <memory>
#include <optional>

#include "buffers/StringBuffer.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/InternalKind.h"
#include "list/EncodedList.h"
#include "list/ListContainer.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "map/EncodedMap.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "views/NodeView.h"

namespace db {

class Column;

template <typename T>
concept NLViewElement = std::same_as<T, types::String::Primitive>
                     || std::same_as<T, types::Embedding::Primitive>
                     || std::same_as<T, ListView>
                     || std::same_as<T, MapView>
                     || std::same_as<T, ListElementView>
                     || std::same_as<T, MapEntryView>;

template <typename T>
concept NLOptionalViewElement = requires { typename T::value_type; }
                             && std::same_as<T, std::optional<typename T::value_type>>
                             && NLViewElement<typename T::value_type>;

// Invoke handler with a column whose rows are views - strings, lists, maps, embeddings, list
// or map cells, present or nullable - typed on its container, when handler takes that
// container. A column of values is left alone.
template <typename Handler>
class NLViewColumnVisitor {
public:
    explicit NLViewColumnVisitor(Handler& handler)
        : _handler(handler)
    {
    }

    void operator()(const ColumnMask* column) {
    }

    template <template <typename> typename Container, typename ElementType>
    void operator()(const Container<ElementType>* column) {
        constexpr bool holdsViews = NLViewElement<ElementType> || NLOptionalViewElement<ElementType>;

        if constexpr (holdsViews) {
            if constexpr (std::invocable<Handler&, const Container<ElementType>*>) {
                _handler(column);
            }
        }
    }

private:
    Handler& _handler;
};

template <typename Handler>
void dispatchViewColumn(const Column* column, Handler&& handler) {
    using Visitor = NLViewColumnVisitor<Handler>;

    Visitor visitor(handler);
    ColumnSingleDispatcher<InternalKind::Types::Tuple, Visitor>::dispatch(column, visitor);
}

// Owns the copies of the values an op keeps past the step that built them. A string, list,
// map or embedding built per row lives in a buffer its op empties when it runs again, so
// an op holding rows across steps - a sort, a join, an aggregate - keeps them in here.
// Most stores hold values of one kind or none, so each buffer is created on first use.
class NLValueStore {
public:
    NLValueStore();
    ~NLValueStore();

    types::String::Primitive own(types::String::Primitive text);
    types::Embedding::Primitive own(types::Embedding::Primitive embedding);
    ListView own(ListView list);
    MapView own(MapView map);
    ListElementView own(ListElementView element);
    MapEntryView own(MapEntryView entry);

    // A list or map a change holds encoded, decoded into this store
    ListView decode(const EncodedList& list);
    MapView decode(const EncodedMap& map);

    // The copy of an item staged for a list, and of a tagged cell holding it
    ListBuffer<>::ListItemVariant ownItem(const ListBuffer<>::ListItemVariant& item);
    ListElementView ownCell(ListBuffer<>::ListItemVariant& item);

    // Copy the views of rows [firstRow, end) of a column in, whatever its kind: a constant
    // is copied whole, and a column of values is left as it is
    void ownColumnRows(Column* column, size_t firstRow);

    // The copy of a column element: a view, present or not, is copied in, and any other
    // value is its own copy already
    template <typename ElementType>
    ElementType ownElement(const ElementType& element) {
        if constexpr (NLViewElement<ElementType>) {
            return own(element);
        } else if constexpr (NLOptionalViewElement<ElementType>) {
            if (!element.has_value()) {
                return std::nullopt;
            }

            return own(*element);
        } else {
            return element;
        }
    }

    // The values copied in since the last clear
    size_t getValueCount() const { return _valueCount; }

    void clear();

private:
    size_t _valueCount {0};
    std::unique_ptr<StringBuffer> _strings;
    std::unique_ptr<ListContainer::EmbeddingBuffer> _embeddings;
    std::unique_ptr<ListContainer> _lists;

    StringBuffer& getStrings();
    ListContainer::EmbeddingBuffer& getEmbeddings();
    ListContainer& getLists();
};

// The two stores a running value is copied into - a min/max extreme, a reduce
// accumulator. A replaced value leaves its copy behind in the active store; once the
// copies outnumber twice the live values, the live ones are copied into the spare store,
// emptied first, and the stores flip, so memory follows the values kept rather than the
// rows folded.
class NLCompactingValueStore {
public:
    NLValueStore& getActive() { return _stores[_active]; }

    bool needsCompaction(size_t liveCount) const;

    NLValueStore& getSpare();

    // Empty the active store and make the spare one active
    void flip();

    void clear();

private:
    NLValueStore _stores[2];
    size_t _active {0};
};

}
