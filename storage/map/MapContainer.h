#pragma once

#include <stddef.h>
#include <memory>
#include <span>
#include <vector>

#include "MapBuffer.h"
#include "MapView.h"

#include "buffers/SpanBuffer.h"
#include "buffers/StringBuffer.h"
#include "list/ListContainer.h"

#include "metadata/PropertyType.h"

namespace db {

class DataPartMerger;

/**
 * @brief Owning store of map values, holding one @ref MapView per stored map.
 *
 * The map counterpart of @ref ListContainer: keys, string and embedding payloads, list
 * values and nested maps are all copied into buffers this container owns. Entries are
 * stored sorted by key, so two equal maps hold their entries in the same order.
 */
class MapContainer {
public:
    friend DataPartMerger;

    using MapKeyValuePair = MapBuffer<>::MapKeyValuePair;
    using ViewVector = std::vector<MapView>;
    using EmbeddingBuffer = SpanBuffer<float, types::Embedding::Primitive>;

    MapContainer();
    ~MapContainer();

    MapContainer(const MapContainer&) = delete;
    MapContainer& operator=(const MapContainer&) = delete;
    MapContainer(MapContainer&& other) noexcept;
    MapContainer& operator=(MapContainer&& other) noexcept;

    /// Copies @param map into this container and stores it as the next value
    void alloc(MapView map);

    /**
     * @brief Copies @param entries into this container, sorted by key, and returns a view
     * of them, without storing that view as a value of its own.
     *
     * Keys and string and embedding payloads are copied in; a @ref ListView value must come
     * from @ref getLists and a nested @ref MapView from this container.
     */
    MapView insert(std::span<const MapKeyValuePair> entries);

    /// Stores an already-owned view, as returned by @ref insert, as the next value
    void append(MapView map);

    /// Copies @param map into this container and returns a view of the copy, without
    /// storing that view as a value of its own
    MapView copy(MapView map);

    /// The store the list values of this container's maps live in
    ListContainer& getLists() { return _lists; }

    const MapView& getView(size_t index) const { return _views[index]; }

    size_t size() const { return _views.size(); }

    const ViewVector& get() const { return _views; }

    ViewVector::const_iterator begin() const { return _views.begin(); }
    ViewVector::const_iterator end() const { return _views.end(); }

    void clear();

private:
    std::unique_ptr<MapBuffer<>> _maps;
    std::unique_ptr<StringBuffer> _strings;
    std::unique_ptr<EmbeddingBuffer> _embeddings;
    ListContainer _lists;
    ViewVector _views;

    MapKeyValuePair own(const MapKeyValuePair& entry);
};

}
