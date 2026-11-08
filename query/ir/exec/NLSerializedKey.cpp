#include "NLSerializedKey.h"

#include <algorithm>

using namespace db;

namespace {

constexpr size_t INITIAL_CAPACITY = 64;

}

NLSerializedKey::NLSerializedKey()
    : _bytes(std::make_unique<char[]>(INITIAL_CAPACITY)),
    _capacity(INITIAL_CAPACITY)
{
}

NLSerializedKey::~NLSerializedKey() {
}

void NLSerializedKey::grow(size_t size) {
    const size_t capacity = std::max(2 * _capacity, _size + size);

    std::unique_ptr<char[]> grown = std::make_unique<char[]>(capacity);
    memcpy(grown.get(), _bytes.get(), _size);

    _bytes.swap(grown);
    _capacity = capacity;
}
