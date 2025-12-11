#pragma once

#include <numeric>
#include <span>
#include <vector>
#include <stdint.h>
#include <tuple>

#include "FaissIdx.h"
#include "LSHSignature.h"

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

    [[nodiscard]] std::span<const LSHSignature> shards() const {
        return _shardSig;
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

    void reset() {
        _shardSig.clear();
        _indices.clear();
        _distances.clear();
        _resultCount = 0;
    }

    void addResult(LSHSignature shardSig, FaissIdx index, float distance) {
        _shardSig.push_back(shardSig);
        _indices.push_back(index);
        _distances.push_back(distance);
        _resultCount++;
    }

    void finishSearch(size_t maxResultCount) {
        std::vector<size_t> sortIdx(_distances.size());
        std::iota(sortIdx.begin(), sortIdx.end(), 0);

        std::sort(sortIdx.begin(), sortIdx.end(), [this](size_t a, size_t b) {
            return _distances[a] < _distances[b];
        });

        std::vector<size_t> shardIdx(_shardSig.size());
        std::vector<FaissIdx> indices(_indices.size());
        std::vector<float> distances(_distances.size());

        for (size_t i = 0; i < _distances.size(); i++) {
            shardIdx[i] = _shardSig[sortIdx[i]];
            indices[i] = _indices[sortIdx[i]];
            distances[i] = _distances[sortIdx[i]];
        }

        _shardSig = std::move(shardIdx);
        _indices = std::move(indices);
        _distances = std::move(distances);

        if (_resultCount <= maxResultCount) {
            return;
        }

        _shardSig.resize(maxResultCount);
        _indices.resize(maxResultCount);
        _distances.resize(maxResultCount);
        _resultCount = maxResultCount;
    }

private:
    std::vector<LSHSignature> _shardSig;
    std::vector<FaissIdx> _indices;
    std::vector<float> _distances;
    size_t _resultCount {0};
};

}
