#include "VecLib.h"

#include <faiss/Index.h>
#include <faiss/IndexFlat.h>
#include <faiss/index_io.h>

#include "BatchVectorCreate.h"
#include "BatchVectorSearch.h"
#include "LSHShardRouter.h"
#include "VecLibShard.h"

#include "TuringTime.h"
#include "VectorException.h"
#include "BioAssert.h"
#include "VectorSearchResult.h"

using namespace vec;

VecLib::Builder::Builder() {
    _vecLib = std::unique_ptr<VecLib>(new VecLib);
}

std::unique_ptr<VecLib> VecLib::Builder::build() {
    auto& storageManager = _vecLib->_storage;

    msgbioassert(storageManager, "VecLib storage must be set");
    msgbioassert(meta._id.valid(), "VecLib identifier must be set");
    msgbioassert(meta._dimension > 0, "VecLib dimension must be set");

    if (auto res = storageManager->createLibraryStorage(*_vecLib); !res) {
        throw VectorException(res.error().fmtMessage());
    }

    auto& meta = _vecLib->_metadata;

    // TODO: sync on disk
    constexpr uint8_t nbits = 12;
    _vecLib->_shardRouter = std::make_unique<LSHShardRouter>(meta._dimension, nbits);
    _vecLib->_shardRouter->initialize();

    const auto now = Clock::now()
                         .time_since_epoch()
                         .count();

    _vecLib->_metadata._createdAt = now;
    _vecLib->_metadata._modifiedAt = now;

    return std::move(_vecLib);
}

VectorResult<void> VecLib::addEmbeddings(const BatchVectorCreate& batch) {
    const float* embeddings = batch.embeddings().data();
    const size_t dim = batch.dimension();

    for (size_t i = 0; i < batch.count(); i++) {
        std::span<const float> vector(embeddings + i * dim, dim);

        LSHSignature signature = _shardRouter->getSignature(vector);
        auto& shard = getShard(signature);

        shard._index->add(1, embeddings + i * dim);
        shard._vecCount++;
    }

    return {};
}
VectorResult<void> VecLib::search(const BatchVectorSearch& query, VectorSearchResult& results) {
    VecLibStorage& storage = _storage->getStorage(_metadata._id);
    msgbioassert(!storage._shards.empty(), "VecLib storage must have at least one shard");

    const float* embeddings = query.embeddings().data();
    const size_t queryCount = query.count();
    const size_t resultCount = query.resultCount();
    msgbioassert(queryCount != 0, "Must have at least one query");

    for (auto& [signature, shard] : storage._shards) {
        if (!shard->_isLoaded) {
            shard->load();
        }

        auto [distances, indices] = results.prepareNewQuery(queryCount);

        const size_t k = std::min(resultCount, (size_t)shard->_index->ntotal);
        shard->_index->search(queryCount, embeddings, k, distances.data(), indices.data());

        if (shard->_isFull) {
            shard->unload();
        }
    }

    results.finishSearch(resultCount);

    return {};
}

VecLibShard& VecLib::getShard(LSHSignature signature) {
    auto& storage = _storage->getStorage(_metadata._id);
    
    auto it = storage._shards.find(signature);
    if (it != storage._shards.end()) {
        return *it->second;
    }

    auto shard = std::make_unique<VecLibShard>();
    shard->_path = _storage->getShardPath(_metadata._id, signature);

    switch (_metadata._metric) {
        case DistanceMetric::EUCLIDEAN_DIST:
            shard->_index = std::make_unique<faiss::IndexFlatL2>(_metadata._dimension);
            break;
        case DistanceMetric::INNER_PRODUCT:
            shard->_index = std::make_unique<faiss::IndexFlatIP>(_metadata._dimension);
            break;
    }

    shard->_dim = _metadata._dimension;

    storage._shards.emplace(signature, std::move(shard));

    return *storage._shards.at(signature);
}

const VecLibShard& VecLib::getShard(LSHSignature signature) const {
    return *_storage->getStorage(_metadata._id)._shards.at(signature);
}

VecLib::VecLib() {
}
