// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <stddef.h>
#include <iterator>

template <class ElementType>
class IteratorBase {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = ElementType;
    using difference_type = std::ptrdiff_t;
    using pointer = ElementType*;
    using reference = ElementType&;

protected:
    IteratorBase() = default;
    ~IteratorBase() = default;
};

template <class STLType>
class STLRangeIterator : public IteratorBase<typename STLType::value_type> {
public:
    using STLTypeIterator = typename STLType::const_iterator;

    STLRangeIterator() = default;

    STLRangeIterator(const STLType* container, bool beginOrEnd)
        : _container(container)
    {
        _it = beginOrEnd ? _container->begin() : _container->end();
    }

    bool isValid() const {
        return _container && _it != _container->end();
    }

    const typename STLType::value_type& operator*() const { return *_it; }

    bool operator==(const STLRangeIterator& other) const {
        return _container == other._container && this->_it == other._it;
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
    using value_type = typename STLType::value_type;

    STLRange() = default;

    STLRange(const STLType* container)
        : _container(container)
    {}

    size_t size() const {
        return _container ? _container->size() : 0;
    }

    bool empty() const {
        return _container ? _container->empty() : true;
    }

    Iterator begin() const {
        return STLRangeIterator(_container, true);
    }

    Iterator end() const {
        return STLRangeIterator(_container, false);
    }

private:
    const STLType* _container {nullptr};
};

template <class STLIndexType>
class STLIndexIterator : public IteratorBase<typename STLIndexType::mapped_type> {
public:
    STLIndexIterator() = default;
    STLIndexIterator(const STLIndexType* container, bool beginOrEnd)
        : _container(container)
    {
        _it = beginOrEnd ? _container->begin() : _container->end();
    }

    bool isValid() const {
        return _container && _it != _container->end();
    }

    const typename STLIndexType::mapped_type& operator*() const { return (*_it).second; }

    bool operator==(const STLIndexIterator& other) const {
        return _container == other._container && this->_it == other._it;
    }

    bool operator!=(const STLIndexIterator& other) const {
        return !(*this == other);
    }

    STLIndexIterator& operator++() { ++_it; return *this; }

private:
    const STLIndexType* _container {nullptr}; 
    typename STLIndexType::const_iterator _it;
};

template <class STLIndexType>
class STLIndexRange {
public:
    using value_type = typename STLIndexType::mapped_type;
    using Iterator = STLIndexIterator<STLIndexType>;

    STLIndexRange() = default;

    STLIndexRange(const STLIndexType* container)
        : _container(container)
    {}

    size_t size() const {
        return _container ? _container->size() : 0;
    }

    bool empty() const {
        return _container ? _container->empty() : true;
    }

    Iterator begin() const {
        return STLIndexIterator(_container, true);
    }

    Iterator end() const {
        return STLIndexIterator(_container, false);
    }

private:
    const STLIndexType* _container {nullptr};
};

template <class BaseRange, typename Flattener>
class FlatIterator 
    : public IteratorBase<typename std::result_of
        <Flattener(typename BaseRange::Iterator)>::type::value_type> {
public:
    FlatIterator() = delete;
    FlatIterator(const BaseRange& baseRange, const Flattener& flattener, bool beginOrEnd)
        : _baseIt(beginOrEnd ? baseRange.begin() : baseRange.end()),
        _endIt(baseRange.end()),
        _flattener(flattener)
    {
        if (_baseIt.isValid()) {
            _flattenIt = _flattener(_baseIt).begin();
        }
    }

    bool isValid() const {
        return _baseIt.isValid() && _flattenIt.isValid();
    }

    const auto& operator*() const { return *_flattenIt; }

    bool operator==(const FlatIterator& other) const {
        if (_baseIt == _endIt) {
            return other._baseIt == _endIt;
        }
        return _baseIt == other._baseIt && _flattenIt == other._flattenIt;
    }

    bool operator!=(const FlatIterator& other) const {
        return !(*this == other);
    }

    FlatIterator& operator++() {
        ++_flattenIt;
        if (!_flattenIt.isValid()) {
            ++_baseIt;
            if (_baseIt.isValid()) {
                _flattenIt = _flattener(_baseIt).begin();
            }
        }
        return *this;
    }

private:
    typename BaseRange::Iterator _baseIt;
    typename BaseRange::Iterator _endIt;
    typename std::result_of<Flattener(typename BaseRange::Iterator)>::type::Iterator _flattenIt;
    const Flattener& _flattener;
};

template <class BaseRange, class Flattener>
class FlatRange {
public:
    using Iterator = FlatIterator<BaseRange, Flattener>;
    using value_type = typename Iterator::value_type;

    FlatRange(BaseRange baseRange, const Flattener& flattener)
        : _baseRange(baseRange), _flattener(flattener)
    {
    }

    size_t size() const {return _baseRange.size(); }

    bool empty() const { return _baseRange.empty(); }

    Iterator begin() const {
        return FlatIterator(_baseRange, _flattener, true);
    }

    Iterator end() const {
        return FlatIterator(_baseRange, _flattener, false);
    }

private:
    BaseRange _baseRange;
    Flattener _flattener;
};

template <class BaseRange, typename Map>
class MappedIterator
    : public IteratorBase<typename std::result_of
        <Map(typename BaseRange::value_type)>::type> {
public:
    MappedIterator(const BaseRange& baseRange, const Map& mapFunc, bool beginOrEnd)
        : _baseIt(beginOrEnd ? baseRange.begin() : baseRange.end()),
        _mapFunc(mapFunc)
    {
    }

    bool isValid() const {
        return _baseIt.isValid();
    }

    auto operator*() const { return _mapFunc(*_baseIt); }

    bool operator==(const MappedIterator& other) const {
        return _baseIt == other._baseIt;
    }

    bool operator!=(const MappedIterator& other) const {
        return !(*this == other);
    }

    MappedIterator& operator++() {
        ++_baseIt;
        return *this;
    }

private:
    typename BaseRange::Iterator _baseIt;
    const Map& _mapFunc;
};

template <class BaseRange, typename Map>
class MappedRange {
public:
    using Iterator = MappedIterator<BaseRange, Map>;
    using value_type = typename Iterator::value_type;

    MappedRange(const BaseRange& baseRange, const Map& mapFunc)
        : _baseRange(baseRange), _mapFunc(mapFunc)
    {
    }

    bool empty() const { return _baseRange.empty(); }

    Iterator begin() const {
        return MappedIterator(_baseRange, _mapFunc, true);
    }

    Iterator end() const {
        return MappedIterator(_baseRange, _mapFunc, false);
    }

private:
    BaseRange _baseRange;
    Map _mapFunc;
};
