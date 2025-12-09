#pragma once

#include <numeric>
#include <span>
#include <vector>
#include <stdint.h>
#include <tuple>

#include "FaissIdx.h"

namespace vec {

class VectorSearchResult {
public:
    VectorSearchResult() = default;
    ~VectorSearchResult() = default;

    VectorSearchResult(const VectorSearchResult&) = delete;
    VectorSearchResult& operator=(const VectorSearchResult&) = delete;
    VectorSearchResult(VectorSearchResult&&) = delete;
    VectorSearchResult& operator=(VectorSearchResult&&) = delete;

    [[nodiscard]] size_t count() const {
        return _indices.size();
    }

    [[nodiscard]] std::span<const size_t> shards() const {
        return _shardIdx;
    }

    [[nodiscard]] std::span<const FaissIdx> indices() const {
        return _indices;
    }

    [[nodiscard]] std::span<const float> distances() const {
        return _distances;
    }

    [[nodiscard]] std::span<FaissIdx> indices() {
        return _indices;
    }

    [[nodiscard]] std::span<float> distances() {
        return _distances;
    }

    [[nodiscard]] std::tuple<std::span<float>, std::span<FaissIdx>> prepareNewQuery(size_t queryCount) {
        const size_t offset = _shardIdx.size();
        const size_t shardIdx = _nextShardIdx++;
        _shardIdx.resize(_shardIdx.size() + queryCount);

        for (size_t i = 0; i < queryCount; i++) {
            _shardIdx[offset + i] = shardIdx;
        }

        _indices.resize(_indices.size() + queryCount);
        _distances.resize(_distances.size() + queryCount);

        std::span indices {_indices};
        std::span distances {_distances};

        return {
            distances.subspan(offset, queryCount),
            indices.subspan(offset, queryCount),
        };
    }

    void finishSearch(size_t resultCount) {
        std::vector<size_t> sortIdx(_distances.size());
        std::iota(sortIdx.begin(), sortIdx.end(), 0);

        std::sort(sortIdx.begin(), sortIdx.end(), [this](size_t a, size_t b) {
            return _distances[a] < _distances[b];
        });

        std::vector<size_t> shardIdx(_shardIdx.size());
        std::vector<FaissIdx> indices(_indices.size());
        std::vector<float> distances(_distances.size());

        for (size_t i = 0; i < _distances.size(); i++) {
            shardIdx[i] = _shardIdx[sortIdx[i]];
            indices[i] = _indices[sortIdx[i]];
            distances[i] = _distances[sortIdx[i]];
        }

        _shardIdx = std::move(shardIdx);
        _indices = std::move(indices);
        _distances = std::move(distances);

        _shardIdx.resize(resultCount);
        _indices.resize(resultCount);
        _distances.resize(resultCount);
    }

private:
    size_t _nextShardIdx {0};
    std::vector<size_t> _shardIdx;
    std::vector<FaissIdx> _indices;
    std::vector<float> _distances;
};

}
