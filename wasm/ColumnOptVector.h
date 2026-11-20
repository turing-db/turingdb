// Copyright Turing Biosystems Ltd
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <stdint.h>
#include <utility>
#include <vector>

#include "ProtoDecodeSink.h"

#include "Column.h"

#include "Bitmask.h"

namespace wasm {

// A nullable vector in the split layout of db::ColumnOptVector: one value per row and a
// validity bit per row, so both buffers go to JS as they are. A null row holds T {}.
template <typename T>
class ColumnOptVector : public Column {
public:
    using ValueType = T;
    using ValidityMask = DynamicLargeBitMask<uint64_t>;

    ColumnOptVector(ColumnType type)
        : Column(ColumnKind::OPTIONAL_VECTOR, type)
    {
    }

    size_t size() const override { return _values.size(); }

    bool isValid(size_t row) const { return _validity.test(row); }

    void set(size_t row, const T& value) {
        _values[row] = value;
        _validity.set(row);
    }

    void set(size_t row, T&& value) {
        _values[row] = std::move(value);
        _validity.set(row);
    }

    std::vector<T>& getValues() { return _values; }
    const std::vector<T>& getValues() const { return _values; }
    T* getChunkValues() { return _values.data(); }
    const ValidityMask& getValidity() const { return _validity; }

    void setValidity(const ValidityMask& validity) { _validity = validity; }

    void resize(size_t size) {
        _values.resize(size);
        _validity.resize(size);
    }

    void clear() override {
        _values.clear();
        _validity.resize(0);
    }

private:
    std::vector<T> _values;
    ValidityMask _validity {0};
};

static_assert(net::proto::OptionalVectorColumn<ColumnOptVector<double>, double>);
static_assert(net::proto::OptionalBufferColumn<ColumnOptVector<double>, double>);

}
