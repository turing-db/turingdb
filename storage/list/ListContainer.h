#pragma once

#include <stddef.h>
#include <memory>
#include <span>
#include <vector>

#include "ListBuffer.h"
#include "ListView.h"

#include "buffers/SpanBuffer.h"
#include "buffers/StringBuffer.h"

#include "metadata/PropertyType.h"

namespace db {

class DataPartMerger;

/**
 * @brief Owning store of list values, holding one @ref ListView per stored list.
 *
 * A @ref ListBuffer on its own does not own a list: a string or embedding element keeps
 * only a view of its payload, and a list handed over by a query points into buffers that
 * query frees. Everything that enters here is copied into buffers this container owns -
 * payloads into @ref _strings and @ref _embeddings, nested lists recursively - so the
 * views it hands out stay valid for as long as it does, as @ref StringContainer's and
 * @ref EmbeddingContainer's do for theirs.
 */
class ListContainer {
public:
    friend DataPartMerger;

    using ListItemVariant = ListBuffer<>::ListItemVariant;
    using ViewVector = std::vector<ListView>;
    using EmbeddingBuffer = SpanBuffer<float, types::Embedding::Primitive>;

    ListContainer();
    ~ListContainer();

    ListContainer(const ListContainer&) = delete;
    ListContainer& operator=(const ListContainer&) = delete;
    ListContainer(ListContainer&& other) noexcept;
    ListContainer& operator=(ListContainer&& other) noexcept;

    /// Copies @param list into this container and stores it as the next value
    void alloc(ListView list);

    /**
     * @brief Copies @param elements into this container and returns a view of them,
     * without storing that view as a value of its own.
     *
     * String and embedding payloads are copied in; a nested @ref ListView element must
     * already have been returned by this container, so build a list depth-first.
     */
    ListView insert(std::span<const ListItemVariant> elements);

    /// Stores an already-owned view, as returned by @ref insert, as the next value
    void append(ListView list);

    const ListView& getView(size_t index) const { return _views[index]; }

    size_t size() const { return _views.size(); }

    const ViewVector& get() const { return _views; }

    ViewVector::const_iterator begin() const { return _views.begin(); }
    ViewVector::const_iterator end() const { return _views.end(); }

    void clear();

private:
    std::unique_ptr<ListBuffer<>> _lists;
    std::unique_ptr<StringBuffer> _strings;
    std::unique_ptr<EmbeddingBuffer> _embeddings;
    ViewVector _views;

    ListView copy(ListView list);
    ListItemVariant own(const ListItemVariant& element);
};

}
