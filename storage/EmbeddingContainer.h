#pragma once

#include <stddef.h>
#include <span>
#include <vector>

#include "EmbeddingBucket.h"
#include "metadata/PropertyType.h"

namespace db {
class DataPartMerger;

class EmbeddingContainer {
public:
    using ViewVector = std::vector<types::Embedding::Primitive>;

    explicit EmbeddingContainer(size_t dimension);
    ~EmbeddingContainer();

    EmbeddingContainer(const EmbeddingContainer&) = delete;
    EmbeddingContainer(EmbeddingContainer&&) noexcept = default;
    EmbeddingContainer& operator=(const EmbeddingContainer&) = delete;

    EmbeddingContainer& operator=(EmbeddingContainer&& other) noexcept {
        _buckets = std::move(other._buckets);
        _views = std::move(other._views);
        return *this;
    }

    void alloc(types::Embedding::Primitive content);

    const types::Embedding::Primitive& getView(size_t index) const { return _views[index]; }

    size_t getDimension() const { return _dimension; }
    size_t getEmbeddingCount() const { return _views.size(); }

    const ViewVector& get() const { return _views; }

    void clear();

private:
    friend DataPartMerger;

    const size_t _dimension {0};
    std::vector<EmbeddingBucket> _buckets;
    ViewVector _views;
};

}
