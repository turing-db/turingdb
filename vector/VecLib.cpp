#include "VecLib.h"

#include <faiss/Index.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIDMap.h>
#include <faiss/index_io.h>

#include "ShardCache.h"
#include "VecLibShardAccessor.h"
#include "VectorStorageManager.h"
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

VecLib::VecLib()
{
}

VecLib::~VecLib() {
}

VecLib::Builder::Builder()
    : _vecLib(new VecLib())
{
}

VectorResult<std::unique_ptr<VecLib>> VecLib::Builder::build() {
    auto& storageManager = _vecLib->_storage;
    auto& meta = _vecLib->_metadata;

    bioassert(storageManager, "VecLib storage must be set");
    bioassert(_vecLib->_shardCache, "VecLib shard cache must be set");
    bioassert(meta._dimension > 0, "VecLib dimension must be set");

    constexpr uint8_t nbits = 11;
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
    : _vecLib(new VecLib())
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

VectorResult<void> VecLib::addEmbeddings(const BatchVectorCreate* batch) {
    // The BatchVectorCreate stores vectors grouped by LSH signature. We need to iterate
    // using explicit indices rather than simple range-based iteration because the batch
    // is a sparse array - vectors are stored at indices matching their LSH signature,
    // with empty entries for unused signatures. The signature value is needed to route
    // each vector to the correct shard in the cache.
    LSHSignature signature = 0;
    for (auto it = batch->begin(); it != batch->end(); ++it, ++signature) {
        const auto& data = *it;
        if (data._externalIDs.empty()) {
            continue;
        }

        // Use the actual index (signature) where data is stored
        {
            _shardRouter->registerShardSignature(signature);
            VecLibShardAccessor shard = _shardCache->getShard(_metadata, signature);
            VecLibShard& shardRef = shard.get();

            const size_t count = data._externalIDs.size();
            shardRef._index->add_with_ids(count, data._embeddings.data(), data._externalIDs.data());
        }
    }

    _metadata._modifiedAt = Clock::now().time_since_epoch().count();

    return {};
}

VectorResult<void> VecLib::search(const VectorSearchQuery* query, VectorSearchResult* results) {
    const std::span<const float> embeddings = query->embeddings();
    const size_t maxResultCount = query->resultCount();

    std::vector<LSHSignature> searchSignatures;

    // Compute signature
    _shardRouter->getSearchSignatures(embeddings, searchSignatures);

    results->reset();
    results->setAscendingIsBetter(_metadata._metric == DistanceMetric::EUCLIDEAN_DIST);

    std::vector<float> distances(maxResultCount);
    std::vector<faiss::idx_t> indices(maxResultCount);

    for (const LSHSignature& signature : searchSignatures) {
        const VecLibShardAccessor shard = _shardCache->getShard(_metadata, signature);
        const VecLibShard& shardRef = shard.get();

        if (shardRef._index->ntotal == 0) {
            continue;
        }

        const size_t k = std::min(maxResultCount, (size_t)shardRef._index->ntotal);
        shardRef._index->search(1, embeddings.data(), k, distances.data(), indices.data());

        for (size_t i = 0; i < k; i++) {
            if (indices[i] < 0) {
                break;
            }

            results->addResult(signature, indices[i], distances[i]);
        }
    }

    results->finishSearch(maxResultCount);

    return {};
}

void VecLib::evictAllShards() {
    const auto& shardSignatures = _shardRouter->getInstantiatedShardSignatures();

    for (const LSHSignature sig : shardSignatures) {
        const ShardIdentifier id(_metadata._id, sig);
        _shardCache->evictShard(id);
    }
}

void VecLib::prepareCreateBatch(BatchVectorCreate* batch) {
    batch->init(_shardRouter.get(), _metadata._dimension);
}

const VecLibStorage* VecLib::getStorage() const {
    return &_storage->getStorage(_metadata._id);
}

VecLibAccessor VecLib::access() {
    return VecLibAccessor(this);
}

