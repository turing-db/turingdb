#include "VecLib.h"

#include <faiss/Index.h>
#include <faiss/IndexFlat.h>
#include <faiss/index_io.h>

#include "BatchVectorCreate.h"
#include "BatchVectorSearch.h"
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
    auto& meta = _vecLib->_metadata;
    auto& storageManager = _vecLib->_storage;

    msgbioassert(storageManager, "VecLib storage must be set");
    msgbioassert(meta._id.valid(), "VecLib identifier must be set");
    msgbioassert(meta._dimension > 0, "VecLib dimension must be set");

    if (auto res = storageManager->createLibraryStorage(*_vecLib); !res) {
        throw VectorException(res.error().fmtMessage());
    }

    auto& shard = storageManager->addShard(*_vecLib);

    const auto now = Clock::now()
                         .time_since_epoch()
                         .count();

    _vecLib->_metadata._createdAt = now;
    _vecLib->_metadata._modifiedAt = now;

    shard._path = storageManager->getShardPath(meta._id, 0);

    return std::move(_vecLib);
}

VectorResult<void> VecLib::addEmbeddings(const BatchVectorCreate& batch) {
    VecLibStorage& storage = _storage->getStorage(_metadata._id);
    msgbioassert(!storage._shards.empty(), "VecLib storage must have at least one shard");

    auto* shard = &storage._shards.back();
    msgbioassert(shard->_isLoaded, "Shard must be loaded");

    size_t remainingPoints = batch.count();
    const float* embeddings = batch.embeddings().data();

    while (remainingPoints > 0) {
        if ((int)batch.dimension() != shard->_index->d) {
            return VectorError::result(VectorErrorCode::InvalidDimension);
        }

        size_t pointsToAdd = std::min(shard->getAvailPointCount(), remainingPoints);

        if (pointsToAdd == 0) {
            // Allocate a new shard
            shard = &_storage->addShard(*this);
            continue;
        }

        shard->_index->add(pointsToAdd, embeddings);
        shard->_vecCount += pointsToAdd;
        embeddings += pointsToAdd * shard->_index->d;
        remainingPoints -= pointsToAdd;
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

    for (VecLibShard& shard : storage._shards) {
        if (!shard._isLoaded) {
            shard.load();
        }

        auto [distances, indices] = results.prepareNewQuery(queryCount);

        const size_t k = std::min(resultCount, (size_t)shard._index->ntotal);
        shard._index->search(queryCount, embeddings, k, distances.data(), indices.data());

        if (shard._isFull) {
            shard.unload();
        }
    }

    results.finishSearch(resultCount);

    return {};
}

const VecLibShard& VecLib::getShard(size_t index) const {
    msgbioassert(index < _metadata._shardCount, "Shard index out of bounds");
    return _storage->getStorage(_metadata._id)._shards.at(index);
}

VecLib::VecLib() {
}
