#pragma once

#include <stddef.h>
#include <algorithm>

template <typename TypeT>
class Slice {
public:
    using pointer_type = const TypeT*;
    using iterator = pointer_type;
    using const_iterator = iterator;

    template <typename ContainerT>
    Slice(const ContainerT& container)
        : _start(std::data(container)),
        _end(_start + std::size(container))
    {
    }

    Slice(pointer_type start, pointer_type end)
        : _start(start),
        _end(end)
    {
    }

    std::size_t size() const { return std::max(0, _end - _start); }
    bool empty() const { return _start >= _end; }

    const TypeT& first() const { return *_start; }
    Slice tail() const { return Slice(_start+1, _end); }

    inline iterator begin() const { return _start; }
    inline const_iterator cbegin() const { return begin(); }

    inline iterator end() const { return _end; }
    inline const_iterator cend() const { return end(); }

private:
    pointer_type _start;
    pointer_type _end;
};
