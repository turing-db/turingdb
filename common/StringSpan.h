#pragma once

#include <stddef.h>

class StringSpan {
public:
    StringSpan() = default;

    StringSpan(char* data, size_t size)
        : _data(data),
        _size(size)
    {
    }

    bool isEmpty() const { return !_data || _size == 0; }

    char* getData() const { return _data; }
    size_t getSize() const { return _size; }

private:
    char* _data {nullptr};
    size_t _size {0};
};
