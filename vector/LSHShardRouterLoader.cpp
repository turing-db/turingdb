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

    _reader.read();

    if (_reader.errorOccured()) {
        return VectorError::result<void>(VectorErrorCode::CouldNotLoadShardRouterFile, _reader.error());
    }

    const fs::ByteBuffer& buffer = _reader.getBuffer();

    fs::ByteBufferIterator it = _reader.iterateBuffer();
    CHECK_VALID(it, VectorErrorCode::ShardRouterFileEmpty);

    // Reading nbits
    router._nbits = it.get<uint8_t>();
    CHECK_VALID(it, VectorErrorCode::ShardRouterInvalidDimension);

    if (router._nbits < 2 || router._nbits > 16) {
        return VectorError::result<void>(VectorErrorCode::ShardRouterInvalidBitCount);
    }

    // Reading dim
    router._dim = it.get<uint64_t>(); // dim
    CHECK_VALID(it, VectorErrorCode::ShardRouterInvalidVectors);

    if (router._dim == 0 || router._dim > 32ull * 1024) {
        return VectorError::result<void>(VectorErrorCode::InvalidDimension);
    }

    // Check buffer size
    const size_t lshRouterSize = sizeof(uint8_t)
                               + sizeof(uint64_t)
                               + sizeof(float) * router._dim * router._nbits;

    if (buffer.size() != lshRouterSize) {
        // If we switch to writing whole pages, this check
        // needs to become if (buffer.size() < lshRouterSize)
        return VectorError::result<void>(VectorErrorCode::ShardRouterInvalidVectors);
    }

    router._hyperplanes.resize(router._nbits);

    for (auto& hyperplane : router._hyperplanes) {
        hyperplane.resize(router._dim);

        for (float& value : hyperplane) {
            value = it.get<float>();
        }
    }

    return {};
}
