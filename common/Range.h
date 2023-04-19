// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <stddef.h>
#include <map>

template <class STLType>
class STLRangeIterator {
public:
    using STLTypeIterator = typename STLType::const_iterator;

    STLRangeIterator() = delete;

    STLRangeIterator(const STLType* container, bool beginOrEnd)
        : _container(container)
    {
        if (_container) {
            _it = beginOrEnd ? _container->begin() : _container->end();
        }
    }

    bool isValid() const {
        return _it != _container->end();
    }

    typename STLType::value_type operator*() const { return *_it; }

    bool operator==(const STLRangeIterator& other) const {
        return this->_it == other._it;
    }

    bool operator!=(const STLRangeIterator& other) const {
        return !(*this == other);
    }

    STLRangeIterator& operator++() { ++_it; return *this; }

private:
    const STLType* _container {nullptr};
    STLTypeIterator _it;
};

template <class STLType>
class STLRange {
public:
    using Iterator = STLRangeIterator<STLType>;
    using ValueType = typename STLType::value_type;

    STLRange() = default;

    STLRange(const STLType* container)
        : _container(container)
    {}

    STLRange(const STLRange& other) = delete;
    STLRange(STLRange&& other) = delete;

    size_t size() const { return _container->size(); }

    bool empty() const { return _container->empty(); }

    Iterator begin() const {
        return STLRangeIterator(_container, true);
    }

    Iterator end() const {
        return STLRangeIterator(_container, false);
    }

private:
    const STLType* _container {nullptr};
};

template <class KeyType, class ValueType>
class STLValueMapIterator {
public:
    using STLMap = std::map<KeyType, ValueType>;
    using STLMapIterator = typename STLMap::const_iterator;
    using iterator_category = std::input_iterator_tag;
    using value_type = ValueType;
    using difference_type = std::ptrdiff_t;
    using pointer = ValueType*;
    using reference = ValueType&;

    STLValueMapIterator() = delete;
    STLValueMapIterator(const STLMap* map, bool beginOrEnd)
        : _map(map)
    {
        if (_map) {
            _it = beginOrEnd ? _map->begin() : _map->end();
        }
    }

    bool isValid() const {
        return _it != _map->end();
    }

    ValueType operator*() const { return (*_it).second; }

    bool operator==(const STLValueMapIterator& other) const {
        return this->_it == other._it;
    }

    bool operator!=(const STLValueMapIterator& other) const {
        return !(*this == other);
    }

    STLValueMapIterator& operator++() { ++_it; return *this; }

private:
    const STLMap* _map {nullptr};
    STLMapIterator _it;
};

template <class KeyType, class ValueType>
class STLValueMapRange {
public:
    using Iterator = STLValueMapIterator<KeyType, ValueType>;

    STLValueMapRange() = default;
    STLValueMapRange(const std::map<KeyType, ValueType>* map)
        : _map(map)
    {}

    size_t size() const { return _map->size(); }

    bool empty() const { return _map->empty(); }

    Iterator begin() const {
        return STLValueMapIterator(_map, true);
    }

    Iterator end() const {
        return STLValueMapIterator(_map, false);
    }

private:
    const std::map<KeyType, ValueType>* _map;
};
