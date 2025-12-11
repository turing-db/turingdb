#include "VecLib.h"

#include <faiss/Index.h>
#include <faiss/IndexFlat.h>
#include <faiss/index_io.h>

#include "ShardCache.h"
#include "StorageManager.h"
#include "BatchVectorCreate.h"
#include "BatchVectorSearch.h"
#include "LSHShardRouter.h"
#include "VecLibMetadataLoader.h"
#include "LSHShardRouterLoader.h"
#include "VecLibShard.h"

#include "TuringTime.h"
#include "VectorException.h"
#include "BioAssert.h"
#include "VectorSearchResult.h"

using namespace vec;

VecLib::~VecLib() {
}

VecLib::Builder::Builder() {
    _vecLib = std::unique_ptr<VecLib>(new VecLib);
}

std::unique_ptr<VecLib> VecLib::Builder::build() {
    auto& storageManager = _vecLib->_storage;
    auto& meta = _vecLib->_metadata;

    msgbioassert(storageManager, "VecLib storage must be set");
    msgbioassert(_vecLib->_shardCache, "VecLib shard cache must be set");
    msgbioassert(meta._dimension > 0, "VecLib dimension must be set");

    constexpr uint8_t nbits = 8;
    _vecLib->_shardRouter = std::make_unique<LSHShardRouter>(meta._dimension, nbits);
    _vecLib->_shardRouter->initialize();

    if (auto res = storageManager->createLibraryStorage(*_vecLib); !res) {
        throw VectorException(res.error().fmtMessage());
    }

    const auto now = Clock::now()
                         .time_since_epoch()
                         .count();

    _vecLib->_metadata._createdAt = now;
    _vecLib->_metadata._modifiedAt = now;

    return std::move(_vecLib);
}

VecLib::Loader::Loader() {
    _vecLib = std::unique_ptr<VecLib>(new VecLib);
}

VectorResult<std::unique_ptr<VecLib>> VecLib::Loader::load(VecLibStorage& storage) {
    auto& meta = _vecLib->_metadata;

    msgbioassert(_vecLib->_storage, "VecLib storage must be set");
    msgbioassert(_vecLib->_shardCache, "VecLib shard cache must be set");

    _vecLib->_shardRouter = std::make_unique<LSHShardRouter>(0, 0);

    VecLibMetadataLoader loader;
    loader.setFile(&storage._metadataFile);
    if (auto res = loader.load(meta); !res) {
        return res.get_unexpected();
    }

    LSHShardRouterLoader routerLoader;
    routerLoader.setFile(&storage._shardRouterFile);
    if (auto res = routerLoader.load(*_vecLib->_shardRouter); !res) {
        return res.get_unexpected();
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

        // Add all ids to the shard
        shard._ids.insert(shard._ids.end(),
                                  data._externalIDs.begin(),
                                  data._externalIDs.end());

        // Add vectors to index
        shard._index->add(data._externalIDs.size(), data._embeddings.data());

        _shardCache->updateMemUsage();
    }

    return {};
}

VectorResult<void> VecLib::search(const BatchVectorSearch& query, VectorSearchResult& results) {
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

        const float memUsage = (float)shard.getUsedMem() / 1024.0f / 1024.0f;
        fmt::println("  * Probing shard {} with {} vectors ({} MiB)", signature, shard._index->ntotal, memUsage);

        if (shard._index->ntotal == 0) {
            fmt::println("      No vectors found");
            continue;
        }

        const size_t k = std::min(maxResultCount, (size_t)shard._index->ntotal);
        shard._index->search(1, embeddings.data(), k, distances.data(), indices.data());

        for (size_t i = 0; i < k; i++) {
            if (indices[i] == std::numeric_limits<faiss::idx_t>::max()) {
                break;
            }

            results.addResult(signature, indices[i], distances[i]);
        }
    }

    results.finishSearch(maxResultCount);

    return {};
}

BatchVectorCreate VecLib::prepareCreateBatch(Dimension dimension) {
    return BatchVectorCreate(*_shardRouter, dimension);
}

const VecLibStorage& VecLib::getStorage() const {
    return _storage->getStorage(_metadata._id);
}

VecLib::VecLib() {
}
