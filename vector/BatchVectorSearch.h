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

    void addPoint(std::span<const float> newPoint) {
        if (newPoint.size() != _dimension) {
            throw VectorException("Vector dimension mismatch");
        }

        const size_t newSize = _embeddings.size() + newPoint.size();

        // Manuall growth, because it's not clear if std::vector<>::insert() will do it for us
        if (_embeddings.capacity() < newSize) {
            _embeddings.reserve(std::max(_embeddings.capacity() * 2, newSize));
        }

        _embeddings.insert(_embeddings.end(), newPoint.begin(), newPoint.end());
    }

    void addPoints(std::span<const float> newPoints) {
        msgbioassert(!newPoints.empty(), "newPoints must not be empty");

        if (newPoints.size() % _dimension != 0) {
            throw VectorException("Vector dimension mismatch");
        }

        const size_t newPointsSize = _embeddings.size() + newPoints.size();

        // Manuall growth, because it's not clear if std::vector<>::insert() will do it for us
        if (_embeddings.capacity() < newPointsSize) {
            _embeddings.reserve(std::max(_embeddings.capacity() * 2, newPointsSize));
        }

        _embeddings.insert(_embeddings.end(), newPoints.begin(), newPoints.end());
    }

    void setResultCount(size_t count) {
        _resultCount = count;
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
        return _resultCount;
    }

private:
    const Dimension _dimension;
    std::vector<float> _embeddings;
    size_t _resultCount {1};
};

}
