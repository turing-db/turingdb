#pragma once

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "NumpyBuffer.h"
#include "NumpyColumn.h"
#include "NumpyElement.h"
#include "NumpyStringArray.h"

#include "ProtoDecodeSink.h"
#include "TuringProtoDecoderConcepts.h"

namespace pybindings {

// The decoder addresses rows relative to the dataframe it is decoding, so every accessor
// is offset by _chunkStart, the row the current dataframe starts at.
template <typename T>
class NumpyColumnVector : public NumpyColumn {
public:
    using ValueType = T;

    NumpyColumnVector() {
    }

    ~NumpyColumnVector() override {
    }

    size_t size() const { return _storage.size() - _chunkStart; }

    T* data() { return _storage.data() + _chunkStart; }

    T& operator[](size_t index) { return _storage[_chunkStart + index]; }

    void resize(size_t size) { _storage.resize(_chunkStart + size); }
    void reserve(size_t size) { _storage.reserve(_chunkStart + size); }

    template <typename... Args>
    T& emplace_back(Args&&... args) { return _storage.emplace_back(std::forward<Args>(args)...); }

    void finishChunk() override { _chunkStart = _storage.size(); }

    const char* getDtypeName() const override { return getNumpyDtypeName<T>(); }

    nb::object toPython(size_t rowCount, const ValueToPyObject& visitor) override {
        using Element = typename ElementOf<T>::Type;

        const size_t size = _storage.size();
        _chunkStart = 0;

        if constexpr (std::is_same_v<Element, db::types::String::Primitive>) {
            nb::object array = makeStringArray(std::span<const T>(_storage.data(), size));
            _storage.clear();
            return array;
        } else if constexpr (std::is_same_v<Element, db::types::Embedding::Primitive>) {
            nb::object values = embeddingsAsMatrix(std::span<const T>(_storage.data(), size));
            if (!values.is_valid()) {
                values = buildList(size, [this, &visitor](size_t row) {
                    return toPyObject(_storage[row], visitor);
                });
            }

            _storage.clear();
            return values;
        } else if constexpr (std::is_same_v<T, db::ValueType>) {
            std::vector<std::string_view> names(size);
            std::transform(_storage.data(), _storage.data() + size, names.begin(), [](db::ValueType value) {
                return db::ValueTypeName::value(value);
            });
            return makeStringArray(names);
        } else if constexpr (isOptional<T> && isTemporal<Element>) {
            NumpyBuffer<int64_t> values;
            values.resize(size);
            std::transform(_storage.data(), _storage.data() + size, values.data(), [](const T& value) {
                return toArrayValue(value);
            });
            return wrapAsNdarray(values.release(), size);
        } else if constexpr (isOptional<T> && hasNumpyArrayType<Element>) {
            NumpyBuffer<NumpyArrayType<Element>> values;
            NumpyBuffer<bool> mask;
            values.resize(size);
            mask.resize(size);

            for (size_t row = 0; row < size; row++) {
                const T& value = _storage[row];
                values[row] = toArrayValue(value);
                mask[row] = !value.has_value();
            }

            _storage.clear();
            return wrapAsMaskedArray(values.release(), mask.release(), size);
        } else if constexpr (!isOptional<T> && hasNumpyArrayType<T>) {
            using Array = NumpyArrayType<T>;

            if constexpr (sizeof(T) == sizeof(Array)) {
                return wrapAsNdarray(reinterpret_cast<Array*>(_storage.release()), size);
            } else {
                NumpyBuffer<Array> values;
                values.resize(size);
                std::transform(_storage.data(), _storage.data() + size, values.data(), [](const T& value) {
                    return toArrayValue(value);
                });
                return wrapAsNdarray(values.release(), size);
            }
        } else {
            nb::object list = buildList(size, [this, &visitor](size_t row) {
                return toPyObject(_storage[row], visitor);
            });
            _storage.clear();
            return list;
        }
    }

private:
    // Fixed-width rows land in a block NumPy takes over; the rest are converted row by row.
    using Storage = std::conditional_t<net::proto::TrivialInternalTypes<T>, NumpyBuffer<T>, std::vector<T>>;

    Storage _storage;
    size_t _chunkStart {0};
};

static_assert(net::proto::VectorColumn<NumpyColumnVector<uint64_t>, uint64_t>);
static_assert(net::proto::OptionalVectorColumn<NumpyColumnVector<std::optional<double>>, double>);

}
