#pragma once

#include <faiss/Index.h>
#include <faiss/index_io.h>

#include "BioAssert.h"
#include "Path.h"

namespace vec {

struct VecLibShard {
    static constexpr size_t MAX_MEM = 256ul * 1024 * 1024; // 256 MB

    fs::Path _path;
    std::unique_ptr<faiss::Index> _index;
    size_t _dim = 0;
    size_t _vecCount = 0;
    bool _isFull = false;
    bool _isLoaded = true;

    void load() {
        _index.reset(faiss::read_index(_path.c_str()));
        _isLoaded = true;
    }

    void unload() {
        _index.reset();
        _isLoaded = false;
    }

    [[nodiscard]] size_t getAvailMem() const {
        const size_t memUsage = sizeof(float) * _dim * _vecCount;

        msgbioassert(memUsage <= MAX_MEM, "Shard memory usage exceeds maximum");

        return MAX_MEM - memUsage;
    }

    [[nodiscard]] size_t getAvailPointCount() const {
        const size_t availMem = getAvailMem();
        return availMem / sizeof(float) / _dim;
    }

    [[nodiscard]] size_t getUsedMem() const {
        return _vecCount * sizeof(float) * _dim;
    }
};

}
