#pragma once

#include <stdint.h>
#include <utility>

#include "ProtoDecodeSink.h"

#include "Column.h"

namespace wasm {

// One value standing for every row, like db::ColumnConst: size() is 0 or 1.
template <typename T>
class ColumnConst : public Column {
public:
    using ValueType = T;

    ColumnConst(ColumnType type)
        : Column(isOptional<T> ? ColumnKind::OPTIONAL_CONSTANT : ColumnKind::CONSTANT, type)
    {
    }

    size_t size() const override { return _empty ? 0 : 1; }

    bool empty() const { return _empty; }
    const T& getValue() const { return _value; }

    void set(const T& value) {
        _value = value;
        _empty = false;
    }

    void set(T&& value) {
        _value = std::move(value);
        _empty = false;
    }

    void clear() override { _empty = true; }

private:
    T _value {};
    bool _empty {true};
};

static_assert(net::proto::ConstColumn<ColumnConst<uint64_t>, uint64_t>);
static_assert(net::proto::OptionalConstColumn<ColumnConst<std::optional<uint64_t>>, uint64_t>);

}
