#include "LSHShardRouterWriter.h"

#include "LSHShardRouter.h"

using namespace vec;

LSHShardRouterWriter::LSHShardRouterWriter()
{
}

LSHShardRouterWriter::~LSHShardRouterWriter() {
}

VectorResult<void> LSHShardRouterWriter::write(const LSHShardRouter& router) {
    if (!_writer.hasFile()) {
        return VectorError::result(VectorErrorCode::WriterNotInitialized);
    }

    if (auto res = _writer.file().clearContent(); !res) {
        return VectorError::result(VectorErrorCode::CouldNotClearShardRouterFile, res.error());
    }

    _writer.write(router._nbits);
    _writer.write(router._dim);

    for (const auto& hyperplane : router._hyperplanes) {
        for (const float value : hyperplane) {
            _writer.write(value);
        }
    }

    _writer.flush();

    if (_writer.errorOccured()) {
        return VectorError::result(VectorErrorCode::CouldNotWriteShardRouterFile, _writer.error());
    }

    return {};
}

