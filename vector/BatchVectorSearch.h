#pragma once

#include <span>
#include <vector>

#include "VecLibMetadata.h"

#include "VectorException.h"
#include "BioAssert.h"

namespace vec {

class BatchVectorSearch {
public:
    explicit BatchVectorSearch(Dimension dimension)
        : _dimension(dimension) {
    }

    ~BatchVectorSearch() = default;

    BatchVectorSearch(const BatchVectorSearch&) = delete;
    BatchVectorSearch& operator=(const BatchVectorSearch&) = delete;
    BatchVectorSearch(BatchVectorSearch&&) = delete;
    BatchVectorSearch& operator=(BatchVectorSearch&&) = delete;

    void setVector(std::span<const float> newPoint) {
        _embeddings.clear();
        _embeddings.insert(_embeddings.end(), newPoint.begin(), newPoint.end());
    }

    void setMaxResultCount(size_t count) {
        _maxResultCount = count;
    }

    [[nodiscard]] Dimension dimension() const {
        return _dimension;
    }

    [[nodiscard]] size_t count() const {
        return _embeddings.size() / _dimension;
    }

    [[nodiscard]] std::span<const float> embeddings() const {
        return _embeddings;
    }

    [[nodiscard]] size_t resultCount() const {
        return _maxResultCount;
    }

private:
    const Dimension _dimension;
    std::vector<float> _embeddings;
    size_t _maxResultCount {1};
};

}
