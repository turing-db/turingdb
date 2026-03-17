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
    explicit EmbeddingContainer(size_t dimension);
    ~EmbeddingContainer();

    EmbeddingContainer(const EmbeddingContainer&) = delete;
    EmbeddingContainer(EmbeddingContainer&&) noexcept = default;
    EmbeddingContainer& operator=(const EmbeddingContainer&) = delete;
    EmbeddingContainer& operator=(EmbeddingContainer&&) noexcept = default;

    void alloc(types::Embedding::Primitive content);

    const types::Embedding::Primitive& getView(size_t index) const { return _views[index]; }

    size_t getDimension() const { return _dimension; }
    size_t getEmbeddingCount() const { return _count; }

    const std::vector<std::span<const float>>& get() const { return _views; }

    void clear();

private:
    friend DataPartMerger;

    size_t _dimension {0};
    size_t _count {0};
    std::vector<EmbeddingBucket> _buckets;
    std::vector<std::span<const float>> _views;
};

}
