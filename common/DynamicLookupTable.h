#pragma once

#include <vector>

template <typename ValueT, typename DefaultValueGenerator>
class BasicDynamicLookupTable {
public:
    void insert(size_t key, const ValueT& value) {
        growTo(key);
        _tbl[key] = value;
    }

    void insert(size_t key, ValueT&& value) {
        growTo(key);
        _tbl[key] = std::move(value);
    }

public:
    std::vector<ValueT> _tbl;

    void growTo(size_t key) {
        if (key >= _tbl.size()) {
            _tbl.resize(key+1, DefaultValueGenerator::generate());
        }
    }
};

class DynamicLookupTableUtils {
public:

template <typename T>
struct DefaultConstructorInitialValue {
    static inline T generate() {
        return T{};
    }
};

template <typename T>
struct NullInitialValue {
    static inline T* generate() {
        return nullptr;
    }
};

};

template <typename ValueT>
class DynamicLookupTable;

template <typename T>
class DynamicLookupTable<T*> : public BasicDynamicLookupTable<T*,
                                      DynamicLookupTableUtils::NullInitialValue<T>> {
public:
    T* lookup(size_t i) const {
        if (i >= this->_tbl.size()) {
            return nullptr;
        }
        return this->_tbl[i];
    }
};

template <typename T>
requires requires (T s) { s.empty(); }
class DynamicLookupTable<T> : public BasicDynamicLookupTable<T,
                                     DynamicLookupTableUtils::DefaultConstructorInitialValue<T>> {
public:
    const T& lookup(size_t i) const {
        if (i >= this->_tbl.size()) {
            return _empty;
        }
        return this->_tbl[i];
    }

private:
    T _empty {DynamicLookupTableUtils::DefaultConstructorInitialValue<T>::generate()};
};
