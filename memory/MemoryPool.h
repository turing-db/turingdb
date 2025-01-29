#pragma once

#include <vector>

namespace db {

template <typename ObjectT>
struct MemoryBlock;

template <typename ObjectT>
class MemoryPool {
public:
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
        auto* block = _first;
        while (block) {
            auto* nextBlock = block->_next;
            delete block;
            block = nextBlock;
        }
    }

    void clear() {
        // Invoke std::vector clear on each MemoryBlock one by one
        auto* block = _first;
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
    MemoryBlock<ObjectT>* _first {nullptr};
    MemoryBlock<ObjectT>* _last {nullptr};
    MemoryBlock<ObjectT>* _current {nullptr};

    void allocBlock() {
        MemoryBlock<ObjectT>* newBlock = new MemoryBlock<ObjectT>();
        _last->_next = newBlock;
        _last = newBlock;
    }
};

template <typename ObjectT>
struct MemoryBlock {
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
