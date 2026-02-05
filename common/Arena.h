#pragma once

#include <utility>

template <typename T>
class Arena;

template <typename T, std::size_t CAPACITY=64>
class ArenaChunk {
public:
    friend Arena<T>;

    ArenaChunk()
    {
    }

    bool isFull() const { return _size == CAPACITY; }

    template <typename... Args>
    T& emplace_back(Args&&... args) {
        const auto pos = _size;
        _size++;
        return *new (&_data[pos]) T(std::forward<Args...>(args...));
    }

private:
    T _data[CAPACITY];
    std::size_t _size {0};
    ArenaChunk<T>* _next {nullptr};
};

template <typename T>
class Arena {
public:
    [[gnu::noinline]] 
    Arena()
    {
        _latest = new ArenaChunk<T>();
        _first = _latest;
    }

    [[gnu::noinline]]
    ~Arena() {
        auto* cur = _first;
        while (cur) {
            auto* next = cur->_next;
            delete cur;
            cur = next;
        }
    }

    template <typename... ArgsT>
    T& emplace_back(ArgsT&&... args) {
        ArenaChunk<T>* last = getLastChunk();
        return last->emplace_back(std::forward<ArgsT>(args)...);
    }

private:
    ArenaChunk<T>* _latest {nullptr};
    ArenaChunk<T>* _first {nullptr};

    ArenaChunk<T>* getLastChunk() {
        ArenaChunk<T>* last = _latest;
        if (!last->isFull()) {
            return last;
        }

        return grow();
    }

    [[gnu::noinline]]
    ArenaChunk<T>* grow() {
        auto* newArenaChunk = new ArenaChunk<T>();
        _latest->_next = newArenaChunk;
        _latest = newArenaChunk;
        return newArenaChunk;
    }
};
