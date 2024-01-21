#pragma once

#include <optional>

template <class TError, class TValue>
class BasicResult {
public:
    BasicResult(const TValue& value)
        : _value(value)
    {
    }

    BasicResult(const TError& error)
        : _error(error)
    {
    }

    operator bool() const { return !_error.has_value(); }

    bool hasValue() const { return _value.has_value(); }
    bool hasError() const { return _error.has_error(); }

    const TValue& getValue() const { return *_value; }
    const TError& getError() const { return *_error; }

private:
    std::optional<TValue> _value;
    std::optional<TError> _error;
};