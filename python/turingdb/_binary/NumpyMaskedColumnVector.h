#pragma once

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <limits>

#include "NumpyBuffer.h"
#include "NumpyColumn.h"
#include "NumpyElement.h"

namespace pybindings {

// A nullable fixed-width column held as a value block and a mask block (true = null). Its
// decoder (in NumpySink.h) copies the values off the wire in bulk and unpacks the null
// bitmask into the mask, so no std::optional is built per row.
template <typename T>
class NumpyMaskedColumnVector : public NumpyColumn {
public:
    using ValueType = T;

    NumpyMaskedColumnVector() {
    }

    ~NumpyMaskedColumnVector() override {
    }

    size_t size() const { return _values.size() - _chunkStart; }

    void resize(size_t size) {
        _values.resize(_chunkStart + size);
        _mask.resize(_chunkStart + size);
    }

    T* getValues() { return _values.data() + _chunkStart; }
    bool* getMask() { return _mask.data() + _chunkStart; }

    void finishChunk() override { _chunkStart = _values.size(); }

    const char* getDtypeName() const override { return getNumpyDtypeName<T>(); }

    nb::object toPython(size_t rowCount, const ValueToPyObject& visitor) override {
        const size_t size = _values.size();
        _chunkStart = 0;

        if constexpr (isTemporal<T>) {
            int64_t* values = reinterpret_cast<int64_t*>(_values.data());
            const bool* mask = _mask.data();

            for (size_t row = 0; row < size; row++) {
                if (mask[row]) {
                    values[row] = std::numeric_limits<int64_t>::min();
                }
            }

            return wrapAsNdarray(reinterpret_cast<int64_t*>(_values.release()), size);
        } else if constexpr (hasNumpyArrayType<T>) {
            using Array = NumpyArrayType<T>;

            if constexpr (sizeof(T) == sizeof(Array)) {
                return wrapAsMaskedArray(reinterpret_cast<Array*>(_values.release()), _mask.release(), size);
            } else {
                NumpyBuffer<Array> values;
                values.resize(size);
                std::transform(_values.data(), _values.data() + size, values.data(), [](const T& value) {
                    return toArrayValue(value);
                });
                return wrapAsMaskedArray(values.release(), _mask.release(), size);
            }
        } else {
            return buildList(size, [this, &visitor](size_t row) {
                return _mask[row] ? nb::none() : toPyObject(_values[row], visitor);
            });
        }
    }

private:
    NumpyBuffer<T> _values;
    NumpyBuffer<bool> _mask;
    size_t _chunkStart {0};
};

}
