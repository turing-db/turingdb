#pragma once

#include <mutex>

#include <faiss/Index.h>
#include <faiss/index_io.h>

#include "Path.h"
#include "VectorResult.h"

namespace faiss {
struct HNSWIndex;
}

namespace vec {

struct VecLibMetadata;

struct VecLibShard {
    mutable std::mutex _mutex;

    fs::Path _indexPath;

    std::unique_ptr<faiss::Index> _index;

    [[nodiscard]] size_t getUsedMem() const {
        return _index->ntotal * (sizeof(float) * _index->d + sizeof(faiss::idx_t));
    }

    VectorResult<void> save();
    VectorResult<void> load(const VecLibMetadata& meta);
};

}
