#pragma once

#include <faiss/Index.h>
#include <faiss/index_io.h>

#include "FileReader.h"
#include "FileWriter.h"
#include "Path.h"
#include "VectorResult.h"

namespace vec {

struct VecLibMetadata;

struct VecLibShard {
    fs::Path _indexPath;

    fs::File _idsFile;
    fs::FileWriter<4096> _idsWriter;
    fs::FileReader _idsReader;

    std::unique_ptr<faiss::Index> _index;
    std::vector<uint64_t> _ids;

    [[nodiscard]] size_t getUsedMem() const {
        return _index->ntotal * sizeof(float) * _index->d
             + _ids.size() * sizeof(uint64_t);
    }

    VectorResult<void> save();
    VectorResult<void> load(const VecLibMetadata& meta);
};

}
