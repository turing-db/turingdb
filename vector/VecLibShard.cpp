#include "VecLibShard.h"

#include <faiss/IndexFlat.h>
#include <faiss/IndexIDMap.h>
#include <faiss/index_io.h>

#include "Panic.h"
#include "VecLibMetadata.h"

using namespace vec;

VectorResult<void> VecLibShard::save() {
    faiss::write_index(_index.get(), _indexPath.c_str());
    return {};
}

VectorResult<void> VecLibShard::load(const VecLibMetadata& meta) {
    if (_indexPath.exists()) {
        _index.reset(faiss::read_index(_indexPath.c_str(), faiss::IO_FLAG_MMAP));
        return {};
    }

    faiss::IndexFlat* flat = nullptr;

    switch (meta._metric) {
        case DistanceMetric::EUCLIDEAN_DIST:
            flat = new faiss::IndexFlatL2(meta._dimension);
            break;
        case DistanceMetric::INNER_PRODUCT:
            flat = new faiss::IndexFlatIP(meta._dimension);
            break;
        case DistanceMetric::_SIZE:
            panic("VecLibShard: invalid distance metric");
            break;
    }

    auto* idMap = new faiss::IndexIDMap(flat);
    idMap->own_fields = true;
    _index.reset(idMap);

    return {};
}
