#pragma once

#include <span>
#include <vector>

#include "VecLibMetadata.h"

#include "VectorException.h"
#include "BioAssert.h"

namespace vec {

class BatchVectorCreate {
public:
    explicit BatchVectorCreate(Dimension dimension)
        : _dimension(dimension) {
    }

    ~BatchVectorCreate() = default;

    BatchVectorCreate(const BatchVectorCreate&) = delete;
    BatchVectorCreate& operator=(const BatchVectorCreate&) = delete;
    BatchVectorCreate(BatchVectorCreate&&) = delete;
    BatchVectorCreate& operator=(BatchVectorCreate&&) = delete;

    void addPoint(uint64_t externalID, std::span<const float> newPoint) {
        if (newPoint.size() != _dimension) {
            throw VectorException("Vector dimension mismatch");
        }

        _externalIDs.push_back(externalID);

        const size_t newSize = _embeddings.size() + newPoint.size();

        // Manuall growth, because it's not clear if std::vector<>::insert() will do it for us
        if (_embeddings.capacity() < newSize) {
            _embeddings.reserve(std::max(_embeddings.capacity() * 2, newSize));
        }

        _embeddings.insert(_embeddings.end(), newPoint.begin(), newPoint.end());
    }

    void addPoints(std::span<const uint64_t> externalIDs, std::span<const float> newPoints) {
        msgbioassert(!newPoints.empty(), "newPoints must not be empty");

        const Dimension dim = newPoints.size() / externalIDs.size();
        const size_t remainingPoints = newPoints.size() % externalIDs.size();

        if (dim != _dimension || remainingPoints != 0) {
            throw VectorException("Vector dimension mismatch");
        }

        const size_t newIDsSize = _externalIDs.size() + externalIDs.size();
        const size_t newPointsSize = _embeddings.size() + newPoints.size();

        // Manuall growth, because it's not clear if std::vector<>::insert() will do it for us
        if (_externalIDs.capacity() < newIDsSize) {
            _externalIDs.reserve(std::max(_externalIDs.capacity() * 2, newIDsSize));
        }

        if (_embeddings.capacity() < newPointsSize) {
            _embeddings.reserve(std::max(_embeddings.capacity() * 2, newPointsSize));
        }

        _externalIDs.insert(_externalIDs.end(), externalIDs.begin(), externalIDs.end());
        _embeddings.insert(_embeddings.end(), newPoints.begin(), newPoints.end());
    }

    void clear() {
        _externalIDs.clear();
        _embeddings.clear();
    }

    [[nodiscard]] Dimension dimension() const {
        return _dimension;
    }

    [[nodiscard]] size_t count() const {
        return _externalIDs.size();
    }

    [[nodiscard]] std::span<const uint64_t> externalIDs() const {
        return _externalIDs;
    }

    [[nodiscard]] std::span<const float> embeddings() const {
        return _embeddings;
    }

    std::vector<uint64_t>& externalIDs() {
        return _externalIDs;
    }

    std::vector<float>& embeddings() {
        return _embeddings;
    }

private:
    const Dimension _dimension;
    std::vector<uint64_t> _externalIDs;
    std::vector<float> _embeddings;
};

}
