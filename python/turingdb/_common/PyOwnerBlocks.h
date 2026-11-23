#pragma once

#include <stddef.h>
#include <stdint.h>
#include <span>
#include <vector>

#include <nanobind/nanobind.h>

namespace pybindings {

namespace nb = nanobind;

// The blocks of T the values of a result view, each a read-only numpy array that every
// view of it keeps alive. A block the decoder filled is taken over as it is. Values held
// anywhere else are copied into a block of copies.
template <typename T>
class PyOwnerBlocks {
public:
    struct Block {
        nb::object _array;
        const T* _data {nullptr};
        size_t _size {0};
    };

    PyOwnerBlocks();
    ~PyOwnerBlocks();

    // Takes every block. A view keeps all of its block's capacity alive, so a block at most
    // half full is not kept, and the values in it are copied.
    void adopt(std::vector<std::vector<T>>& blocks);

    // The block holding values, which start at offset in it
    const Block& place(std::span<const T> values, size_t& offset);

private:
    std::vector<Block> _adopted;
    std::vector<std::vector<T>> _unadopted;
    size_t _lastAdopted {0};

    Block _copies;
    T* _copyData {nullptr};
    size_t _copyCount {0};

    const Block* findAdopted(const T* values);
    void allocCopies(size_t valueCount);
};

}
