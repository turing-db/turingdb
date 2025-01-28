#pragma once

#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <spdlog/fmt/ostr.h>

namespace db {

template <typename T>
concept IntegralType = (std::is_integral_v<T>);

template <IntegralType T>
class ID {
public:
    using Type = T;

    ID() = default;

    ID(T id)
        : _id(id)
    {
    }

    ID& operator=(T id)
    {
        _id = id;
        return *this;
    }

    bool operator==(const ID& other) const {
        return _id == other._id;
    }

    bool operator!=(const ID& other) const {
        return !(*this == other);
    }

    bool operator<(const ID& other) const {
        return _id < other._id;
    }

    bool operator<=(const ID& other) const {
        return _id <= other._id;
    }

    bool operator>=(const ID& other) const {
        return _id >= other._id;
    }

    bool operator>(const ID& other) const {
        return _id > other._id;
    }

    ID operator+(const ID& other) const {
        return ID(_id + other._id);
    }

    ID operator-(const ID& other) const {
        return ID(_id - other._id);
    }

    ID& operator++() {
        ++_id;
        return *this;
    }

    ID operator++(int) {
        ID temp(*this);
        ++*this;
        return temp;
    }

    ID& operator+=(T inc) {
        _id += inc;
        return *this;
    }

    ID& operator+=(const ID& other) {
        _id += other._id;
        return *this;
    }

    T getValue() const { return _id; }

    bool isValid() const { return _id != _max; }

    static constexpr ID max() { return ID(); }
    static constexpr size_t _max = std::numeric_limits<T>::max();

private:
    T _id {_max};
};

using EntityID = ID<uint64_t>;
using PropertyID = ID<uint64_t>;
using EdgeTypeID = ID<uint64_t>;
using PropertyTypeID = ID<uint16_t>;
using LabelID = ID<uint64_t>;
using LabelSetID = ID<uint32_t>;

}

template <typename T>
struct std::hash<db::ID<T>> {
    size_t operator()(const db::ID<T>& id) const {
        return std::hash<size_t> {}(id.getValue());
    }
};

namespace std {

template <typename T>
inline string to_string(db::ID<T> id) {
    return to_string(id.getValue());
}

template <typename T>
ostream& operator<<(ostream& os, db::ID<T> id) {
    return os << id.getValue();
}

}

template <typename T> struct fmt::formatter<db::ID<T>> : ostream_formatter {};
