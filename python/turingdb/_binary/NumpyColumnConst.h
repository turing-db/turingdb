#pragma once

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <optional>
#include <string_view>
#include <utility>

#include "NumpyBuffer.h"
#include "NumpyColumn.h"
#include "NumpyElement.h"
#include "NumpyStringArray.h"

#include "ProtoDecodeSink.h"

namespace pybindings {

// One value standing for every row of the result.
template <typename T>
class NumpyColumnConst : public NumpyColumn {
public:
    using ValueType = T;

    NumpyColumnConst() {
    }

    ~NumpyColumnConst() override {
    }

    void set(const T& value) { _value = value; }
    void set(T&& value) { _value = std::move(value); }

    void finishChunk() override {
    }

    const char* getDtypeName() const override { return getNumpyDtypeName<T>(); }

    nb::object toPython(size_t rowCount, const ValueToPyObject& visitor) override {
        using Element = typename ElementOf<T>::Type;

        constexpr bool isArray = isOptional<T> ? isTemporal<Element> : hasNumpyArrayType<T>;

        if constexpr (std::is_same_v<Element, db::types::String::Primitive>) {
            return makeStringArray(std::optional<std::string_view>(_value), rowCount);
        } else if constexpr (std::is_same_v<Element, db::types::Embedding::Primitive>) {
            const db::types::Embedding::Primitive* embedding = getEmbedding(_value);
            if (rowCount == 0 || !embedding) {
                const nb::object value = toPyObject(_value, visitor);
                return buildList(rowCount, [&value](size_t) {
                    return value;
                });
            }

            const size_t dimension = embedding->size();

            NumpyBuffer<float> values;
            values.resize(rowCount * dimension);

            for (size_t row = 0; row < rowCount; row++) {
                std::copy(embedding->begin(), embedding->end(), values.data() + row * dimension);
            }

            return wrapAsMatrix(values.release(), rowCount, dimension);
        } else if constexpr (isOptional<T> && hasNumpyArrayType<Element> && !isArray) {
            NumpyBuffer<NumpyArrayType<Element>> values;
            NumpyBuffer<bool> mask;
            values.resize(rowCount);
            mask.resize(rowCount);

            std::fill_n(values.data(), rowCount, toArrayValue(_value));
            std::fill_n(mask.data(), rowCount, !_value.has_value());

            return wrapAsMaskedArray(values.release(), mask.release(), rowCount);
        } else if constexpr (isArray) {
            NumpyBuffer<NumpyArrayType<Element>> values;
            values.resize(rowCount);
            std::fill_n(values.data(), rowCount, toArrayValue(_value));
            return wrapAsNdarray(values.release(), rowCount);
        } else {
            const nb::object value = toPyObject(_value, visitor);
            return buildList(rowCount, [&value](size_t) {
                return value;
            });
        }
    }

private:
    T _value {};
};

static_assert(net::proto::ConstColumn<NumpyColumnConst<uint64_t>, uint64_t>);
static_assert(net::proto::OptionalConstColumn<NumpyColumnConst<std::optional<uint64_t>>, uint64_t>);

}
