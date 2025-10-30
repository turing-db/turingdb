#pragma once

#include <vector>

namespace db {

template <typename ObjectT>
struct MemoryBlock;

template <typename ObjectT>
class MemoryPool {
public:
    using MemoryBlockT = MemoryBlock<ObjectT>;

    MemoryPool()
        : _first(new MemoryBlock<ObjectT>()),
        _last(_first),
        _current(_first)
    {
    }

    MemoryPool(const MemoryPool&) = delete;
    MemoryPool(MemoryPool&&) = delete;
    MemoryPool& operator=(const MemoryPool&) = delete;
    MemoryPool& operator=(MemoryPool&&) = delete;

    ~MemoryPool() {
        MemoryBlockT* block = _first;
        while (block) {
            MemoryBlockT* nextBlock = block->_next;
            delete block;
            block = nextBlock;
        }
    }

    void clear() {
        // Invoke std::vector clear on each MemoryBlock one by one
        MemoryBlockT* block = _first;
        while (block) {
            block->_objects.clear();
            block = block->_next;
        }
        _current = _first;
    }

    template <typename... ArgsT>
    inline ObjectT* alloc(ArgsT&&... args) {
        const size_t currentCapacity = _current->_objects.capacity();
        if (currentCapacity == 0) {
            if (!_current->_next) {
                allocBlock();
            }
            _current = _current->_next;
        }

        return &_current->_objects.emplace_back(std::forward<ArgsT>(args)...);
    }

private:
    MemoryBlockT* _first {nullptr};
    MemoryBlockT* _last {nullptr};
    MemoryBlockT* _current {nullptr};

    void allocBlock() {
        MemoryBlockT* newBlock = new MemoryBlock<ObjectT>();
        _last->_next = newBlock;
        _last = newBlock;
    }
};

template <typename ObjectT>
struct MemoryBlock {
    // Each block is 8MB in capacity
    static constexpr size_t MEMORY_BLOCK_BYTE_CAPACITY = 8*1024*1024;
    static constexpr size_t MEMORY_BLOCK_CAPACITY = MEMORY_BLOCK_BYTE_CAPACITY/sizeof(ObjectT);
    static_assert(sizeof(ObjectT) <= MEMORY_BLOCK_BYTE_CAPACITY);

    MemoryBlock()
    {
        _objects.reserve(MEMORY_BLOCK_CAPACITY);
    }

    std::vector<ObjectT> _objects;
    MemoryBlock<ObjectT>* _next {nullptr};
};

}
