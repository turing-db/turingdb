#pragma once

#include <span>
#include <unordered_map>
#include <vector>

#include "LSHShardRouter.h"
#include "LSHSignature.h"
#include "VecLibMetadata.h"

#include "VectorException.h"
#include "BioAssert.h"
#include "spdlog/fmt/bundled/base.h"

namespace vec {

class BatchVectorCreate {
public:
    struct Data {
        std::vector<uint64_t> _externalIDs;
        std::vector<float> _embeddings;
    };

    BatchVectorCreate(LSHShardRouter& router, Dimension dimension)
        : _dimension(dimension),
          _router(&router) {
    }

    ~BatchVectorCreate() = default;

    BatchVectorCreate(const BatchVectorCreate&) = delete;
    BatchVectorCreate& operator=(const BatchVectorCreate&) = delete;
    BatchVectorCreate(BatchVectorCreate&&) = delete;
    BatchVectorCreate& operator=(BatchVectorCreate&&) = delete;

    void clear() {
        for (auto& data : _data) {
            data._externalIDs.clear();
            data._embeddings.clear();
        }
    }

    void addPoint(uint64_t externalID, std::span<const float> newPoint) {
        if (newPoint.size() != _dimension) [[unlikely]] {
            throw VectorException("Vector dimension mismatch");
        }

        const LSHSignature signature = _router->getSignature(newPoint);

        if (signature >= _data.size()) {
            _data.resize(signature + 1);
        }

        auto& data = _data[signature];
        data._externalIDs.push_back(externalID);

        const size_t newSize = data._embeddings.size() + newPoint.size();

        // Manuall growth, because it's not clear if std::vector<>::insert() will do it for us
        if (data._embeddings.capacity() < newSize) {
            data._embeddings.reserve(std::max(data._embeddings.capacity() * 2, newSize));
        }

        data._embeddings.insert(data._embeddings.end(), newPoint.begin(), newPoint.end());
    }

    [[nodiscard]] Dimension dimension() const {
        return _dimension;
    }

    [[nodiscard]] size_t count() const {
        size_t count = 0;

        for (const auto& data : _data) {
            count += data._externalIDs.size();
        }

        return count;
    }

    std::vector<Data>::const_iterator begin() const {
        return _data.begin();
    }

    std::vector<Data>::const_iterator end() const {
        return _data.end();
    }

private:
    const Dimension _dimension;
    LSHShardRouter* _router {nullptr};

    std::vector<Data> _data;
};

}
