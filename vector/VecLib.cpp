#include "VecLib.h"

#include <mutex>

#include <faiss/Index.h>
#include <faiss/IndexFlat.h>
#include <faiss/index_io.h>

#include "ShardCache.h"
#include "StorageManager.h"
#include "BatchVectorCreate.h"
#include "VectorSearchQuery.h"
#include "LSHShardRouter.h"
#include "VecLibMetadataLoader.h"
#include "LSHShardRouterLoader.h"
#include "VecLibShard.h"
#include "VecLibAccessor.h"

#include "TuringTime.h"
#include "BioAssert.h"
#include "VectorSearchResult.h"

using namespace vec;

VecLib::~VecLib() {
}

VecLib::Builder::Builder()
    : _vecLib(new VecLib)
{
}

VectorResult<std::unique_ptr<VecLib>> VecLib::Builder::build() {
    auto& storageManager = _vecLib->_storage;
    auto& meta = _vecLib->_metadata;

    bioassert(storageManager, "VecLib storage must be set");
    bioassert(_vecLib->_shardCache, "VecLib shard cache must be set");
    bioassert(meta._dimension > 0, "VecLib dimension must be set");

    constexpr uint8_t nbits = 10;
    _vecLib->_shardRouter = std::make_unique<LSHShardRouter>(meta._dimension, nbits);
    _vecLib->_shardRouter->initialize();

    if (auto res = storageManager->createLibraryStorage(*_vecLib); !res) {
        return nonstd::make_unexpected(res.error());
    }

    const auto now = Clock::now()
                         .time_since_epoch()
                         .count();

    _vecLib->_metadata._createdAt = now;
    _vecLib->_metadata._modifiedAt = now;

    return std::move(_vecLib);
}

VecLib::Loader::Loader()
    : _vecLib(new VecLib)
{
}

VectorResult<std::unique_ptr<VecLib>> VecLib::Loader::load(VecLibStorage& storage) {
    auto& meta = _vecLib->_metadata;

    bioassert(_vecLib->_storage, "VecLib storage must be set");
    bioassert(_vecLib->_shardCache, "VecLib shard cache must be set");

    _vecLib->_shardRouter = std::make_unique<LSHShardRouter>(0, 0);

    VecLibMetadataLoader loader;
    loader.setFile(&storage._metadataFile);
    if (auto res = loader.load(meta); !res) {
        return nonstd::make_unexpected(res.error());
    }

    LSHShardRouterLoader routerLoader;
    routerLoader.setFile(&storage._shardRouterFile);
    if (auto res = routerLoader.load(*_vecLib->_shardRouter); !res) {
        return nonstd::make_unexpected(res.error());
    }

    return std::move(_vecLib);
}

VectorResult<void> VecLib::addEmbeddings(const BatchVectorCreate& batch) {
    LSHSignature signature = 0;
    for (const auto& data : batch) {
        if (data._externalIDs.empty()) {
            continue;
        }

        auto& shard = _shardCache->getShard(_metadata, signature++);

        {
            std::unique_lock lock {shard._mutex};

            // Add all ids to the shard
            shard._ids.insert(shard._ids.end(),
                              data._externalIDs.begin(),
                              data._externalIDs.end());

            // Add vectors to index
            shard._index->add(data._externalIDs.size(), data._embeddings.data());
        }

        _shardCache->updateMemUsage();
    }

    _metadata._modifiedAt = Clock::now().time_since_epoch().count();

    return {};
}

VectorResult<void> VecLib::search(const VectorSearchQuery& query, VectorSearchResult& results) {
    const std::span<const float> embeddings = query.embeddings();
    const size_t maxResultCount = query.resultCount();

    std::vector<LSHSignature> searchSignatures;

    // Compute signature
    _shardRouter->getSearchSignatures(embeddings, searchSignatures);

    results.reset();

    std::vector<float> distances(maxResultCount);
    std::vector<faiss::idx_t> indices(maxResultCount);

    for (const LSHSignature& signature : searchSignatures) {
        auto& shard = _shardCache->getShard(_metadata, signature);
        std::unique_lock lock {shard._mutex};

        if (shard._index->ntotal == 0) {
            continue;
        }

        const size_t k = std::min(maxResultCount, (size_t)shard._index->ntotal);
        shard._index->search(1, embeddings.data(), k, distances.data(), indices.data());

        for (size_t i = 0; i < k; i++) {
            if (indices[i] == std::numeric_limits<faiss::idx_t>::max()) {
                break;
            }

            results.addResult(signature, shard._ids.at(indices[i]), distances[i]);
        }
    }

    results.finishSearch(maxResultCount);

    return {};
}

BatchVectorCreate VecLib::prepareCreateBatch() {
    return BatchVectorCreate(*_shardRouter, _metadata._dimension);
}

const VecLibStorage& VecLib::getStorage() const {
    return _storage->getStorage(_metadata._id);
}

VecLibAccessor VecLib::access() {
    return VecLibAccessor {_mutex, *this};
}

VecLib::VecLib()
{
}
