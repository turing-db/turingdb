#include "PyOwnerBlocks.h"

#include <algorithm>
#include <functional>
#include <utility>

#include "NanobindUtils.h"

using namespace pybindings;

namespace {

constexpr size_t FIRST_COPY_BLOCK_SIZE = 1024;
constexpr size_t MAX_COPY_BLOCK_SIZE = 64 * 1024;

}

template <typename T>
PyOwnerBlocks<T>::PyOwnerBlocks() {
}

template <typename T>
PyOwnerBlocks<T>::~PyOwnerBlocks() {
}

template <typename T>
void PyOwnerBlocks<T>::adopt(std::vector<std::vector<T>>& blocks) {
    for (std::vector<T>& values : blocks) {
        const bool worthKeeping = 2 * values.size() > values.capacity();
        if (!worthKeeping) {
            _unadopted.push_back(std::move(values));
            continue;
        }

        Block& block = _adopted.emplace_back();
        block._data = values.data();
        block._size = values.size();
        block._array = wrapVectorAsNdarrayOf<const T>(std::move(values));
    }

    blocks.clear();
}

template <typename T>
const typename PyOwnerBlocks<T>::Block& PyOwnerBlocks<T>::place(std::span<const T> values, size_t& offset) {
    const Block* adopted = findAdopted(values.data());
    if (adopted) {
        offset = static_cast<size_t>(values.data() - adopted->_data);
        return *adopted;
    }

    if (_copyCount + values.size() > _copies._size) {
        allocCopies(values.size());
    }

    offset = _copyCount;
    std::copy(values.begin(), values.end(), _copyData + offset);
    _copyCount += values.size();

    return _copies;
}

template <typename T>
const typename PyOwnerBlocks<T>::Block* PyOwnerBlocks<T>::findAdopted(const T* values) {
    const std::less<const T*> before;

    for (size_t tried = 0; tried < _adopted.size(); tried++) {
        const size_t index = (_lastAdopted + tried) % _adopted.size();
        const Block& block = _adopted[index];

        const bool inBlock = !before(values, block._data) && before(values, block._data + block._size);
        if (inBlock) {
            _lastAdopted = index;
            return &block;
        }
    }

    return nullptr;
}

template <typename T>
void PyOwnerBlocks<T>::allocCopies(size_t valueCount) {
    const size_t grown = std::min(2 * _copies._size, MAX_COPY_BLOCK_SIZE);
    const size_t size = std::max({valueCount, grown, FIRST_COPY_BLOCK_SIZE});

    std::vector<T> values(size);
    _copyData = values.data();
    _copyCount = 0;

    _copies._data = values.data();
    _copies._size = size;
    _copies._array = wrapVectorAsNdarrayOf<const T>(std::move(values));
}

namespace pybindings {
template class PyOwnerBlocks<float>;
template class PyOwnerBlocks<uint64_t>;
}
