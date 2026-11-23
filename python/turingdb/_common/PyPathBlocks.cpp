#include "PyPathBlocks.h"

#include <algorithm>
#include <functional>
#include <utility>

#include "NanobindUtils.h"
#include "PyGraphPath.h"

using namespace pybindings;

namespace {

constexpr size_t FIRST_COPY_BLOCK_SIZE = 1024;
constexpr size_t MAX_COPY_BLOCK_SIZE = 64 * 1024;

}

PyPathBlocks::PyPathBlocks() {
}

PyPathBlocks::~PyPathBlocks() {
}

void PyPathBlocks::adopt(std::vector<std::vector<uint64_t>>& blocks) {
    for (std::vector<uint64_t>& ids : blocks) {
        const bool worthKeeping = 2 * ids.size() > ids.capacity();
        if (!worthKeeping) {
            _unadopted.push_back(std::move(ids));
            continue;
        }

        Block& block = _adopted.emplace_back();
        block._data = ids.data();
        block._size = ids.size();
        block._array = wrapVectorAsNdarrayOf<const uint64_t>(std::move(ids));
    }

    blocks.clear();
}

nb::object PyPathBlocks::makePath(std::span<const uint64_t> ids) {
    if (ids.empty()) {
        return nb::none();
    }

    const size_t nodeCount = (ids.size() + 1) / 2;

    const Block* adopted = findAdopted(ids.data());
    if (adopted) {
        return PyGraphPath::create(adopted->_array, adopted->_data, static_cast<size_t>(ids.data() - adopted->_data), nodeCount);
    }

    if (_copyCount + ids.size() > _copies._size) {
        allocCopies(ids.size());
    }

    const size_t offset = _copyCount;
    std::copy(ids.begin(), ids.end(), _copyData + offset);
    _copyCount += ids.size();

    return PyGraphPath::create(_copies._array, _copies._data, offset, nodeCount);
}

const PyPathBlocks::Block* PyPathBlocks::findAdopted(const uint64_t* ids) {
    const std::less<const uint64_t*> before;

    for (size_t tried = 0; tried < _adopted.size(); tried++) {
        const size_t index = (_lastAdopted + tried) % _adopted.size();
        const Block& block = _adopted[index];

        const bool inBlock = !before(ids, block._data) && before(ids, block._data + block._size);
        if (inBlock) {
            _lastAdopted = index;
            return &block;
        }
    }

    return nullptr;
}

void PyPathBlocks::allocCopies(size_t idCount) {
    const size_t grown = std::min(2 * _copies._size, MAX_COPY_BLOCK_SIZE);
    const size_t size = std::max({idCount, grown, FIRST_COPY_BLOCK_SIZE});

    std::vector<uint64_t> ids(size);
    _copyData = ids.data();
    _copyCount = 0;

    _copies._data = ids.data();
    _copies._size = size;
    _copies._array = wrapVectorAsNdarrayOf<const uint64_t>(std::move(ids));
}
