#pragma once

#include <optional>

namespace db {

template <typename T>
class OptionalRef {
public:
    explicit OptionalRef(const T* value)
        : _value(value)
    {
    }

    bool has_value() const { return _value != nullptr; }
    explicit operator bool() const { return _value != nullptr; }

    const T& operator*() const { return *_value; }
    const T* operator->() const { return _value; }

    operator std::optional<T>() const {
        if (_value == nullptr) {
            return std::nullopt;
        }

        return *_value;
    }

private:
    const T* _value {nullptr};
};

}
