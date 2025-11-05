#pragma once

#include <vector>
#include <array>
#include <iterator>

template <typename T, size_t SMALL_CAPACITY>
class SmallVector : public std::vector<T> {
public:
    template <typename VectorType, bool Const>
    class Iterator {
    public:
        using value_type = T;
        using reference = T&;
        using const_reference = const T&;
        using pointer = T*;
        using const_pointer = const T*;
        using difference_type = std::ptrdiff_t;
        using size_type = size_t;
        using iterator_category = std::random_access_iterator_tag;

        Iterator() = delete;

        Iterator(VectorType& vector, size_t index)
            : _vector(vector),
            _index(index)
        {}

        Iterator(const Iterator& other)
            : _vector(other._vector),
            _index(other._index)
        {}

        Iterator(Iterator&& other)
            : _vector(other._vector),
            _index(other._index)
        {
            other._index = 0;
        }

        Iterator& operator=(Iterator&& other) {
            _vector = other._vector;
            _index = other._index;
            other._index = 0;
            return *this;
        }

        Iterator& operator=(const Iterator& other) {
            _vector = other._vector;
            _index = other._index;
            return *this;
        }

        bool operator==(const Iterator& other) const {
            return _index == other._index;
        }

        bool operator!=(const Iterator& other) const {
            return _index != other._index;
        }

        bool operator<(const Iterator& other) const {
            return _index < other._index;
        }

        bool operator>(const Iterator& other) const {
            return _index > other._index;
        }

        bool operator<=(const Iterator& other) const {
            return _index <= other._index;
        }

        bool operator>=(const Iterator& other) const {
            return _index >= other._index;
        }

        difference_type operator-(const Iterator& other) const {
            return _index - other._index;
        }

        template <bool IsConst = Const>
        requires IsConst
        const T& operator*() {
            return _vector[_index];
        }

        template <bool IsConst = Const>
        requires (!IsConst)
        T& operator*() {
            return _vector[_index];
        }

        Iterator& operator++() {
            _index++;
            return *this;
        }

        Iterator operator++(int) {
            Iterator temp = *this;
            _index++;
            return temp;
        }

        Iterator& operator--() {
            _index--;
            return *this;
        }

        Iterator operator--(int) {
            Iterator temp = *this;
            _index--;
            return temp;
        }

        Iterator& operator+=(difference_type n) {
            _index += n;
            return *this;
        }

        Iterator operator+(difference_type n) const {
            return Iterator(_vector, _index + n);
        }

        Iterator& operator-=(difference_type n) {
            _index -= n;
            return *this;
        }

        Iterator operator-(difference_type n) const {
            return Iterator(_vector, _index - n);
        }

    private:
        VectorType& _vector;
        size_t _index {0};
    };

    using iterator = Iterator<SmallVector, false>;
    using const_iterator = Iterator<const SmallVector, true>;

    SmallVector()
    {
    }

    ~SmallVector() {
        if (_bigStorage) {
            delete _bigStorage;
            _bigStorage = nullptr;
        }
    }

    template <typename Func>
    inline void map(Func&& func) const {
        const auto& dataArray = _data;
        for (size_t i = 0; i < SMALL_CAPACITY; i++) {
            func(dataArray[i]);
        }

        if (_bigStorage) {
            const auto& bigStorageData = _bigStorage->data;
            for (const auto& value : bigStorageData) {
                func(value);
            }
        }
    }

    iterator begin() { return Iterator<SmallVector, false>(*this, 0); }
    const_iterator begin() const { return Iterator<const SmallVector, true>(*this, 0); }

    iterator end() { return Iterator<SmallVector, false>(*this, _size); }
    const_iterator end() const { return Iterator<const SmallVector, true>(*this, _size); }

    inline T& operator[](size_t index) {
        if (index < SMALL_CAPACITY) {
            return _data[index];
        } else {
            return _bigStorage->data[index - SMALL_CAPACITY];
        }
    }

    inline const T& operator[](size_t index) const {
        if (index < SMALL_CAPACITY) {
            return _data[index];
        } else {
            return _bigStorage->data[index - SMALL_CAPACITY];
        }
    }

    inline size_t size() const {
        return _size;
    }

    inline bool empty() const {
        return _size == 0;
    }

    bool operator==(const SmallVector& other) const {
        return _size == other._size && std::equal(begin(), end(), other.begin());
    }

    bool operator!=(const SmallVector& other) const {
        return !(*this == other);
    }

    bool operator==(const std::vector<T>& other) const {
        return _size == other.size() && std::equal(begin(), end(), other.begin());
    }

    bool operator!=(const std::vector<T>& other) const {
        return !(*this == other);
    }

    void clear() {
        _size = 0;
        if (_bigStorage) {
            _bigStorage->data.clear();
        }
    }

    template <typename Arg>
    void push_back(Arg&& value) {
        const size_t currentSize = _size;
        if (_size < SMALL_CAPACITY) {
            _data[currentSize] = std::forward<Arg>(value);
        } else {
            if (_size == SMALL_CAPACITY) {
                grow();
            }
            _bigStorage->data.push_back(std::forward<Arg>(value));
        }
        _size++;
    }

    template <typename Container>
    void append(const Container& other) {
        if (_bigStorage) {
            _bigStorage->data.insert(_bigStorage->data.end(), other.begin(), other.end());
            _size += other.size();
        } else {
            for (const auto& value : other) {
                push_back(value);
            }
        }
    }

private:
    struct BigStorage {
        std::vector<T> data;
    };

    std::array<T, SMALL_CAPACITY> _data;
    BigStorage* _bigStorage {nullptr};
    size_t _size {0};

    void grow() {
        _bigStorage = new BigStorage();
        _bigStorage->data.reserve(SMALL_CAPACITY);
    }
};
