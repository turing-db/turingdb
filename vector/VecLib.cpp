#include "VecLib.h"

#include <faiss/Index.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexHNSW.h>
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
#include "Panic.h"
#include "VectorSearchResult.h"

using namespace vec;

namespace {

// Builds a new empty HNSW flat index wrapped in an ID map for the given metadata.
std::unique_ptr<faiss::Index> buildHNSWIndex(const VecLibMetadata& meta) {
    constexpr int M = 32;
    constexpr int efConstruction = 40;

    const faiss::MetricType faissMetric = (meta._metric == DistanceMetric::EUCLIDEAN_DIST)
                                              ? faiss::METRIC_L2
                                              : faiss::METRIC_INNER_PRODUCT;

    auto* hnsw = new faiss::IndexHNSWFlat(static_cast<int>(meta._dimension), M, faissMetric);
    hnsw->hnsw.efConstruction = efConstruction;

    auto* idMap = new faiss::IndexIDMap(hnsw);
    idMap->own_fields = true;
    return std::unique_ptr<faiss::Index>(idMap);
}

}

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

    switch (meta._indexType) {
        case IndexType::FLAT: {
            constexpr uint8_t nbits = 11;
            _vecLib->_shardRouter = std::make_unique<LSHShardRouter>(meta._dimension, nbits);
            _vecLib->_shardRouter->initialize();
        }
        break;
        case IndexType::HNSW:
            _vecLib->_hnswIndex = buildHNSWIndex(meta);
        break;
        case IndexType::_SIZE:
            panic("VecLib: invalid index type");
        break;
    }

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

    VecLibMetadataLoader loader;
    loader.setFile(&storage._metadataFile);
    if (auto res = loader.load(meta); !res) {
        return nonstd::make_unexpected(res.error());
    }

    switch (meta._indexType) {
        case IndexType::FLAT: {
            _vecLib->_shardRouter = std::make_unique<LSHShardRouter>(0, 0);

            LSHShardRouterLoader routerLoader;
            routerLoader.setFile(&storage._shardRouterFile);
            if (auto res = routerLoader.load(*_vecLib->_shardRouter); !res) {
                return nonstd::make_unexpected(res.error());
            }
        }
        break;
        case IndexType::HNSW: {
            const fs::Path hnswPath = _vecLib->_storage->getHNSWIndexPath(meta._id);
            if (hnswPath.exists()) {
                _vecLib->_hnswIndex.reset(faiss::read_index(hnswPath.c_str()));
            } else {
                _vecLib->_hnswIndex = buildHNSWIndex(meta);
            }
        }
        break;
        case IndexType::_SIZE:
            panic("VecLib: invalid index type");
        break;
    }

    return std::move(_vecLib);
}

VectorResult<void> VecLib::addEmbeddingsBruteForce(const BatchVectorCreate* batch) {
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

    // Persist the router so the shard signatures just registered survive a restart;
    // exact search iterates the instantiated signature set and would otherwise find
    // nothing after reload.
    if (auto res = _storage->persistShardRouter(*this); !res) {
        return nonstd::make_unexpected(res.error());
    }

    _metadata._modifiedAt = Clock::now().time_since_epoch().count();
    return {};
}

VectorResult<void> VecLib::addEmbeddingsHNSW(const BatchVectorCreate* batch) {
    for (auto it = batch->begin(); it != batch->end(); ++it) {
        const auto& data = *it;
        if (data._externalIDs.empty()) {
            continue;
        }

        const size_t count = data._externalIDs.size();
        _hnswIndex->add_with_ids(count, data._embeddings.data(), data._externalIDs.data());
    }

    if (auto res = _storage->persistHNSWIndex(*this); !res) {
        return nonstd::make_unexpected(res.error());
    }

    _metadata._modifiedAt = Clock::now().time_since_epoch().count();
    return {};
}

VectorResult<void> VecLib::addEmbeddings(const BatchVectorCreate* batch) {
    switch (_metadata._indexType) {
        case IndexType::FLAT:
            return addEmbeddingsBruteForce(batch);
        break;
        case IndexType::HNSW:
            return addEmbeddingsHNSW(batch);
        break;
        case IndexType::_SIZE:
            panic("VecLib: invalid index type");
        break;
    }
    panic("VecLib: invalid index type");
}

VectorResult<void> VecLib::search(const VectorSearchQuery* query, VectorSearchResult* results) {
    const std::span<const float> embeddings = query->embeddings();
    const size_t maxResultCount = query->resultCount();

    results->reset();
    results->setAscendingIsBetter(_metadata._metric == DistanceMetric::EUCLIDEAN_DIST);

    std::vector<float> distances(maxResultCount);
    std::vector<faiss::idx_t> indices(maxResultCount);

    switch (_metadata._indexType) {
        case IndexType::FLAT: {
            const std::set<LSHSignature>& searchSignatures = _shardRouter->getInstantiatedShardSignatures();

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
        }
        break;
        case IndexType::HNSW: {
            if (_hnswIndex && _hnswIndex->ntotal > 0) {
                const size_t k = std::min(maxResultCount, (size_t)_hnswIndex->ntotal);
                _hnswIndex->search(1, embeddings.data(), k, distances.data(), indices.data());

                for (size_t i = 0; i < k; i++) {
                    if (indices[i] < 0) {
                        break;
                    }

                    results->addResult(0, indices[i], distances[i]);
                }
            }
        }
        break;
        case IndexType::_SIZE:
            panic("VecLib: invalid index type");
        break;
    }

    results->finishSearch(maxResultCount);

    return {};
}

void VecLib::evictAllShards() {
    switch (_metadata._indexType) {
        case IndexType::FLAT: {
            const auto& shardSignatures = _shardRouter->getInstantiatedShardSignatures();

            for (const LSHSignature sig : shardSignatures) {
                const ShardIdentifier id(_metadata._id, sig);
                _shardCache->evictShard(id);
            }
            return;
        }
        break;
        case IndexType::HNSW:
            return;
        break;
        case IndexType::_SIZE:
            panic("VecLib: invalid index type");
        break;
    }
}

void VecLib::prepareCreateBatch(BatchVectorCreate* batch) {
    switch (_metadata._indexType) {
        case IndexType::FLAT:
            batch->init(_shardRouter.get(), _metadata._dimension);
            return;
        break;
        case IndexType::HNSW:
            batch->init(nullptr, _metadata._dimension);
            return;
        break;
        case IndexType::_SIZE:
            panic("VecLib: invalid index type");
        break;
    }
}

const VecLibStorage* VecLib::getStorage() const {
    return &_storage->getStorage(_metadata._id);
}

VecLibAccessor VecLib::access() {
    return VecLibAccessor(this);
}

