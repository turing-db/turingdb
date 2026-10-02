#pragma once

#include <stddef.h>
#include <stdlib.h>
#include <algorithm>
#include <new>
#include <type_traits>

namespace pybindings {

// Uninitialised, realloc-grown storage for trivially copyable values. The decoder writes
// rows straight into it, and release() hands the block to a NumPy array, which frees it.
template <typename T>
class NumpyBuffer {
public:
    static_assert(std::is_trivially_copyable_v<T>);

    NumpyBuffer() {
    }

    ~NumpyBuffer() {
        free(_data);
    }

    NumpyBuffer(const NumpyBuffer&) = delete;
    NumpyBuffer& operator=(const NumpyBuffer&) = delete;

    size_t size() const { return _size; }

    T* data() { return _data; }
    const T* data() const { return _data; }

    T& operator[](size_t index) { return _data[index]; }
    const T& operator[](size_t index) const { return _data[index]; }

    void reserve(size_t capacity) {
        if (capacity > _capacity) {
            grow(capacity);
        }
    }

    void resize(size_t size) {
        reserve(size);
        _size = size;
    }

    T& emplace_back(T value) {
        reserve(_size + 1);
        _data[_size] = value;
        return _data[_size++];
    }

    void clear() { _size = 0; }

    // Gives up ownership of the block. It is never null, so NumPy always gets a valid pointer.
    T* release() {
        reserve(1);

        T* data = _data;
        _data = nullptr;
        _size = 0;
        _capacity = 0;
        return data;
    }

private:
    T* _data {nullptr};
    size_t _size {0};
    size_t _capacity {0};

    void grow(size_t minCapacity) {
        const size_t capacity = std::max(minCapacity, 2 * _capacity);

        T* data = static_cast<T*>(realloc(_data, capacity * sizeof(T)));
        if (!data) {
            throw std::bad_alloc();
        }

        _data = data;
        _capacity = capacity;
    }
};

}
