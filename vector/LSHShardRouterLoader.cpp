#include "LSHShardRouterLoader.h"

#include "LSHShardRouter.h"

using namespace vec;

#define CHECK_VALID(x, e)                    \
    if (it == it.end()) {                    \
        return VectorError::result<void>(e); \
    }

LSHShardRouterLoader::LSHShardRouterLoader()
{
}

LSHShardRouterLoader::~LSHShardRouterLoader() {
}

VectorResult<void> LSHShardRouterLoader::load(LSHShardRouter& router) {
    if (!_reader.hasFile()) {
        return VectorError::result<void>(VectorErrorCode::ReaderNotInitialized);
    }

    const size_t fileSize = _reader.file().getInfo()._size;
    size_t remaining = fileSize;

    _reader.read();

    if (_reader.errorOccured()) {
        return VectorError::result<void>(VectorErrorCode::CouldNotLoadShardRouterFile, _reader.error());
    }

    fs::ByteBufferIterator it = _reader.iterateBuffer();
    CHECK_VALID(it, VectorErrorCode::ShardRouterFileEmpty);

    // Reading nbits
    router._nbits = it.get<uint8_t>();

    if (router._nbits < 2 || router._nbits > 16) {
        return VectorError::result<void>(VectorErrorCode::ShardRouterInvalidBitCount);
    }

    CHECK_VALID(it, VectorErrorCode::ShardRouterInvalidDimension);

    // Reading dim
    router._dim = it.get<uint64_t>(); // dim

    if (router._dim == 0 || router._dim > 32ull * 1024) {
        return VectorError::result<void>(VectorErrorCode::InvalidDimension);
    }

    CHECK_VALID(it, VectorErrorCode::ShardRouterInvalidHyperplanes);

    // Reading hyperplanes
    remaining = remaining
              - sizeof(uint8_t)
              - sizeof(uint64_t);

    const size_t hyperplanesSize = sizeof(float) * router._dim * router._nbits;

    if (remaining < hyperplanesSize) {
        return VectorError::result<void>(VectorErrorCode::ShardRouterInvalidHyperplanes);
    }

    remaining -= hyperplanesSize;

    router._hyperplanes.resize(router._nbits);

    for (auto& hyperplane : router._hyperplanes) {
        hyperplane.resize(router._dim);

        for (float& value : hyperplane) {
            value = it.get<float>();
        }
    }

    // Reading shard IDs
    CHECK_VALID(it, VectorErrorCode::ShardRouterInvalidShardIDs);

    const size_t shardCount = it.get<uint64_t>();
    remaining -= sizeof(uint64_t);

    if (remaining < shardCount * sizeof(uint64_t)) {
        return VectorError::result<void>(VectorErrorCode::ShardRouterInvalidShardIDs);
    }

    for (size_t i = 0; i < shardCount; i++) {
        router._instantiatedShardSignatures.emplace(it.get<uint64_t>());
    }

    return {};
}
