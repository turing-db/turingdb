#pragma once

#include <algorithm>
#include <numeric>
#include <span>
#include <vector>
#include <stdint.h>

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
        return _ids.size();
    }

    [[nodiscard]] std::span<const LSHSignature> shards() const {
        return _shardSig;
    }

    [[nodiscard]] std::span<const uint64_t> ids() const {
        return _ids;
    }

    [[nodiscard]] std::span<const float> distances() const {
        return _distances;
    }

    void reset() {
        _shardSig.clear();
        _ids.clear();
        _distances.clear();
        _resultCount = 0;
    }

    void addResult(LSHSignature shardSig, uint64_t id, float distance) {
        _shardSig.push_back(shardSig);
        _ids.push_back(id);
        _distances.push_back(distance);
        _resultCount++;
    }

    void finishSearch(size_t maxResultCount) {
        std::vector<size_t> sortIdx(_distances.size());
        std::iota(sortIdx.begin(), sortIdx.end(), 0);

        std::sort(sortIdx.begin(), sortIdx.end(), [this](size_t a, size_t b) {
            return _distances[a] < _distances[b];
        });

        std::vector<LSHSignature> shardIdx(_shardSig.size());
        std::vector<uint64_t> ids(_ids.size());
        std::vector<float> distances(_distances.size());

        for (size_t i = 0; i < _distances.size(); i++) {
            shardIdx[i] = _shardSig[sortIdx[i]];
            ids[i] = _ids[sortIdx[i]];
            distances[i] = _distances[sortIdx[i]];
        }

        _shardSig = std::move(shardIdx);
        _ids = std::move(ids);
        _distances = std::move(distances);

        if (_resultCount <= maxResultCount) {
            return;
        }

        _shardSig.resize(maxResultCount);
        _ids.resize(maxResultCount);
        _distances.resize(maxResultCount);
        _resultCount = maxResultCount;
    }

private:
    std::vector<LSHSignature> _shardSig;
    std::vector<uint64_t> _ids;
    std::vector<float> _distances;
    size_t _resultCount {0};
};

}
