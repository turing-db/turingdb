#pragma once

#include <stddef.h>
#include <stdint.h>
#include <span>
#include <vector>

#include <nanobind/nanobind.h>

namespace pybindings {

namespace nb = nanobind;

// The blocks of IDs the GraphPath objects of a result view, each a read-only numpy uint64
// array that every GraphPath over it and every view of its nodes or edges keeps alive. A
// block the decoder filled is taken over as it is. The IDs of any other path are copied
// into a block of copies.
class PyPathBlocks {
public:
    PyPathBlocks();
    ~PyPathBlocks();

    // Takes every block. One at most half full is not kept: its paths are copied.
    void adopt(std::vector<std::vector<uint64_t>>& blocks);

    // None for the null path, which has no ID
    nb::object makePath(std::span<const uint64_t> ids);

private:
    struct Block {
        nb::object _array;
        const uint64_t* _data {nullptr};
        size_t _size {0};
    };

    std::vector<Block> _adopted;
    std::vector<std::vector<uint64_t>> _unadopted;
    size_t _lastAdopted {0};

    Block _copies;
    uint64_t* _copyData {nullptr};
    size_t _copyCount {0};

    const Block* findAdopted(const uint64_t* ids);
    void allocCopies(size_t idCount);
};

}
