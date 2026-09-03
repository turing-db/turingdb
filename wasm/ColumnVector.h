#pragma once

#include <stdint.h>
#include <utility>
#include <vector>

#include "ProtoDecodeSink.h"

#include "Column.h"

namespace wasm {

// Header-only template, like db::ColumnVector: definitions must be visible at
// instantiation. The internal type is passed in because it is not derivable from
// T — every ID type on the wire decodes to the same uint64_t representation.
template <typename T>
class ColumnVector : public Column {
public:
    using ValueType = T;

    ColumnVector(ColumnType type)
        : Column(isOptional<T> ? ColumnKind::OPTIONAL_VECTOR : ColumnKind::VECTOR, type)
    {
    }

    size_t size() const override { return _container.size(); }

    T* data() { return _container.data(); }
    const T* data() const { return _container.data(); }

    T& operator[](size_t index) { return _container[index]; }
    const T& operator[](size_t index) const { return _container[index]; }

    template <typename... Args>
    T& emplace_back(Args&&... args) { return _container.emplace_back(std::forward<Args>(args)...); }

    void push_back(const T& val) { _container.push_back(val); }
    void resize(size_t size) { _container.resize(size); }
    void reserve(size_t size) { _container.reserve(size); }
    void clear() override { _container.clear(); }

private:
    std::vector<T> _container;
};

static_assert(net::proto::VectorColumn<ColumnVector<uint64_t>, uint64_t>);
static_assert(net::proto::OptionalVectorColumn<ColumnVector<std::optional<double>>, double>);

}
