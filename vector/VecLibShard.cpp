#include "VecLibShard.h"

#include <faiss/IndexFlat.h>
#include <faiss/index_io.h>

#include "FileReader.h"
#include "FileWriter.h"
#include "VecLibMetadata.h"

using namespace vec;

VectorResult<void> VecLibShard::save() {
    faiss::write_index(_index.get(), _indexPath.c_str());

    if (auto res = _idsFile.clearContent(); !res) {
        return VectorError::result(VectorErrorCode::CouldNotClearExternalIDsFile, res.error());
    }

    _idsWriter.write(std::span {_ids});
    _idsWriter.flush();

    if (_idsWriter.errorOccured()) {
        return VectorError::result(VectorErrorCode::CouldNotWriteExternalIDsFile);
    }

    return {};
}

VectorResult<void> VecLibShard::load(const VecLibMetadata& meta) {
    switch (meta._metric) {
        case DistanceMetric::EUCLIDEAN_DIST:
            _index = std::make_unique<faiss::IndexFlatL2>(meta._dimension);
            break;
        case DistanceMetric::INNER_PRODUCT:
            _index = std::make_unique<faiss::IndexFlatIP>(meta._dimension);
            break;
    }

    if (_indexPath.exists()) {
        // Load shard
        _index.reset(faiss::read_index(_indexPath.c_str(), faiss::IO_FLAG_MMAP));
    }

    fs::File& file = _idsReader.file();
    if (auto res = file.seek(0); !res) {
        return VectorError::result(VectorErrorCode::CouldNotSeekBeginningOfExternalIDsFile, res.error());
    }

    _idsReader.read();

    if (_idsReader.errorOccured()) {
        return VectorError::result(VectorErrorCode::CouldNotReadExternalIDsFile);
    }

    const fs::ByteBuffer& buf = _idsReader.getBuffer();
    const size_t nodeCount = buf.size() / sizeof(uint64_t);

    if (buf.size() % sizeof(uint64_t) != 0) {
        return VectorError::result(VectorErrorCode::ExternalIDsFileInvalid);
    }

    _ids.clear();
    _ids.resize(nodeCount);
    std::memcpy(_ids.data(), buf.data(), buf.size());

    return {};
}

