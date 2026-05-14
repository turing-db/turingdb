#include "BumpPtrAllocator.h"

#include <new>
#include <stdlib.h>
#include <memory>

namespace {

constexpr size_t SLAB_SAFETY_MARGIN = 64;

}

class BumpPtrAllocatorDestructor {
public:
    friend BumpPtrAllocator;

    BumpPtrAllocatorDestructor(void* ptr,
                               const BumpPtrAllocator::DestructFunc& destructFunc,
                               BumpPtrAllocatorDestructor* prev)
        : _prev(prev),
        _ptr(ptr),
        _destructFunc(destructFunc)
    {
    }

    ~BumpPtrAllocatorDestructor() = default;

    void destroy();

private:
    BumpPtrAllocatorDestructor* _prev {nullptr};
    void* _ptr {nullptr};
    BumpPtrAllocator::DestructFunc _destructFunc;
};

class BumpPtrAllocatorSlab {
public:
    friend BumpPtrAllocator;

    explicit BumpPtrAllocatorSlab(size_t slabSize, BumpPtrAllocatorSlab* prev);

    BumpPtrAllocatorSlab(const BumpPtrAllocatorSlab&) = delete;
    BumpPtrAllocatorSlab& operator=(const BumpPtrAllocatorSlab&) = delete;

    ~BumpPtrAllocatorSlab();

    void init();

    void reset();

private:
    BumpPtrAllocatorSlab* _prev {nullptr};
    uint8_t* _data {nullptr};
    uint8_t* _start {nullptr};
    size_t _size {0};
    const size_t _capacity {0};
};

// BumpPtrAllocator

BumpPtrAllocator::BumpPtrAllocator(size_t defaultSlabSize)
    : _defaultSlabSize(defaultSlabSize)
{
}

BumpPtrAllocator::~BumpPtrAllocator() {
    reset();

    // Destroy slabs
    BumpPtrAllocatorSlab* currentSlab = _freeSlab;
    while (currentSlab) {
        BumpPtrAllocatorSlab* prev = currentSlab->_prev;
        delete currentSlab;
        currentSlab = prev;
    }
}

void* BumpPtrAllocator::allocate(size_t size, size_t align) {
    const size_t requiredSlabSize = size+align+SLAB_SAFETY_MARGIN;

    if (!_lastSlab || _lastSlab->_size < requiredSlabSize) {
        // Check first free slab if any
        if (_freeSlab && _freeSlab->_capacity >= requiredSlabSize) {
            // If it fits in the first free slab, move it from the free list
            // to the in-use list.
            BumpPtrAllocatorSlab* reused = _freeSlab;
            _freeSlab = _freeSlab->_prev;
            reused->reset();
            reused->_prev = _lastSlab;
            _lastSlab = reused;
        } else {
            _lastSlab = new BumpPtrAllocatorSlab(std::max(requiredSlabSize, _defaultSlabSize), _lastSlab);
            _lastSlab->init();
        }
    }

    if (void* result = std::align(align, size, (void*&)_lastSlab->_data, _lastSlab->_size)) {
        _lastSlab->_data += size;
        _lastSlab->_size -= size;    
        return result;
    }

    return nullptr;
}

void BumpPtrAllocator::registerDestructor(void* ptr, const DestructFunc& destructFunc) {
   BumpPtrAllocatorDestructor* destr = new (*this) BumpPtrAllocatorDestructor(ptr, destructFunc, _lastDestruct);
   _lastDestruct = destr;
}

void BumpPtrAllocator::reset() {
    // Call registered destructors
    BumpPtrAllocatorDestructor* currentDestr = _lastDestruct;
    while (currentDestr) {
        currentDestr->destroy();
        currentDestr = currentDestr->_prev;
    }

    // Empty the list of destructors
    _lastDestruct = nullptr;

    // Reset the list of slabs
    _freeSlab = _lastSlab;
    _lastSlab = nullptr;
}

// BumpAllocatorSlab

BumpPtrAllocatorSlab::BumpPtrAllocatorSlab(size_t size, BumpPtrAllocatorSlab* prev)
    : _prev(prev),
    _size(size),
    _capacity(size)
{
}

BumpPtrAllocatorSlab::~BumpPtrAllocatorSlab() {
    if (_start) {
        free(_start);
    }
}

void BumpPtrAllocatorSlab::init() {
    void* ptr = malloc(_capacity);
    if (!ptr) {
        throw std::bad_alloc();
    }

    _start = (uint8_t*)ptr;
    _data = _start;
}

void BumpPtrAllocatorSlab::reset() {
    _data = _start;
    _size = _capacity;
}

// BumpPtrAllocatorDestructor

void BumpPtrAllocatorDestructor::destroy() {
    if (_destructFunc && _ptr) {
        _destructFunc(_ptr);
    }
}

