#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <algorithm>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <type_traits>

#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>

#include "NanobindUtils.h"
#include "NumpyBuffer.h"

#include "EntityList.h"
#include "GraphPath.h"
#include "ID.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "map/MapView.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"

#include "TuringException.h"

namespace pybindings {

template <typename T>
inline constexpr bool isOptional = false;

template <typename T>
inline constexpr bool isOptional<std::optional<T>> = true;

template <typename T>
struct ElementOf {
    using Type = T;
};

template <typename T>
struct ElementOf<std::optional<T>> {
    using Type = T;
};

template <typename T>
inline constexpr bool isTemporal = std::is_same_v<T, db::types::DateTime::Primitive>
                                || std::is_same_v<T, db::types::Duration::Primitive>;

// The ndarray element type a column of T becomes; void where it becomes a Python list.
template <typename T>
struct NumpyArrayTypeOf {
    using Type = void;
};

template <>
struct NumpyArrayTypeOf<db::types::UInt64::Primitive> {
    using Type = uint64_t;
};

template <>
struct NumpyArrayTypeOf<db::types::Int64::Primitive> {
    using Type = int64_t;
};

template <>
struct NumpyArrayTypeOf<db::types::Double::Primitive> {
    using Type = double;
};

template <>
struct NumpyArrayTypeOf<db::types::Bool::Primitive> {
    using Type = uint8_t;
};

template <>
struct NumpyArrayTypeOf<db::types::DateTime::Primitive> {
    using Type = int64_t;
};

template <>
struct NumpyArrayTypeOf<db::types::Duration::Primitive> {
    using Type = int64_t;
};

template <>
struct NumpyArrayTypeOf<db::ChangeID> {
    using Type = uint64_t;
};

template <typename T, int I>
struct NumpyArrayTypeOf<db::ID<T, I>> {
    using Type = uint64_t;
};

template <typename T>
using NumpyArrayType = typename NumpyArrayTypeOf<T>::Type;

template <typename T>
inline constexpr bool hasNumpyArrayType = !std::is_void_v<NumpyArrayType<T>>;

template <typename T>
NumpyArrayType<T> toArrayValue(const T& value) {
    if constexpr (std::is_same_v<T, db::types::Bool::Primitive>) {
        return value._boolean ? 1 : 0;
    } else if constexpr (isTemporal<T>) {
        return value.getMicroseconds();
    } else if constexpr (std::is_same_v<T, db::ChangeID>) {
        return value.get();
    } else if constexpr (db::IsID<T>::value) {
        return value.getValue();
    } else {
        return value;
    }
}

// A null temporal is NaT, NumPy's minimum int64. Other null values are hidden by a mask,
// so they are written as 0.
template <typename T>
NumpyArrayType<T> toArrayValue(const std::optional<T>& value) {
    if (value.has_value()) {
        return toArrayValue(*value);
    } else if constexpr (isTemporal<T>) {
        return std::numeric_limits<int64_t>::min();
    } else {
        return 0;
    }
}

template <typename T>
const char* getNumpyDtypeName() {
    using Element = typename ElementOf<T>::Type;

    if constexpr (std::is_same_v<Element, db::types::Int64::Primitive>) {
        return "Int64";
    } else if constexpr (std::is_same_v<Element, db::types::Double::Primitive>) {
        return "Double";
    } else if constexpr (std::is_same_v<Element, db::types::Bool::Primitive>) {
        return "Bool";
    } else if constexpr (std::is_same_v<Element, db::types::DateTime::Primitive>) {
        return "DateTime";
    } else if constexpr (std::is_same_v<Element, db::types::Duration::Primitive>) {
        return "Duration";
    } else if constexpr (hasNumpyArrayType<Element>) {
        return "UInt64";
    } else if constexpr (std::is_same_v<Element, db::types::String::Primitive> || std::is_same_v<Element, db::ValueType>) {
        return "String";
    } else if constexpr (std::is_same_v<Element, db::types::Embedding::Primitive>) {
        return "Embedding";
    } else if constexpr (std::is_same_v<Element, db::EntityList>) {
        return "EntityList";
    } else if constexpr (std::is_same_v<Element, db::ListView>) {
        return "List";
    } else if constexpr (std::is_same_v<Element, db::MapView>) {
        return "Map";
    } else if constexpr (std::is_same_v<Element, db::ListElementView>) {
        return "ListElement";
    } else if constexpr (std::is_same_v<Element, db::PropertyNull>) {
        return "Null";
    } else {
        return "object";
    }
}

template <typename T>
nb::object toPyObject(const T& value, const ValueToPyObject& visitor) {
    if constexpr (std::is_same_v<T, db::types::Bool::Primitive>) {
        return nb::cast(value._boolean);
    } else if constexpr (std::is_same_v<T, db::types::String::Primitive>) {
        return nb::str(value.data(), value.size());
    } else if constexpr (std::is_same_v<T, db::types::Embedding::Primitive>) {
        return embeddingToNdarray(value);
    } else if constexpr (std::is_same_v<T, db::ValueType>) {
        const std::string_view name = db::ValueTypeName::value(value);
        return nb::str(name.data(), name.size());
    } else if constexpr (std::is_same_v<T, db::EntityList>) {
        return entityListToPy(value);
    } else if constexpr (std::is_same_v<T, db::ListView> || std::is_same_v<T, db::MapView>) {
        return visitor.view(value);
    } else if constexpr (std::is_same_v<T, db::ListElementView>) {
        return visitor.element(value);
    } else if constexpr (std::is_same_v<T, db::PropertyNull>) {
        return nb::none();
    } else if constexpr (std::is_same_v<T, db::Path>) {
        throw TuringException("Unsupported column type in query result: Path");
    } else {
        return nb::cast(toArrayValue(value));
    }
}

template <typename T>
nb::object toPyObject(const std::optional<T>& value, const ValueToPyObject& visitor) {
    if (!value.has_value()) {
        return nb::none();
    }

    return toPyObject(*value, visitor);
}

// NumPy frees the block through the capsule once the array is collected.
template <typename T>
nb::object wrapAsNdarray(T* data, size_t size) {
    nb::capsule owner(data, [](void* block) noexcept {
        free(block);
    });

    const size_t shape[1] = {size};
    return nb::cast(nb::ndarray<nb::numpy, T, nb::ndim<1>>(data, 1, shape, owner));
}

template <typename T>
nb::object wrapAsMatrix(T* data, size_t rowCount, size_t columnCount) {
    nb::capsule owner(data, [](void* block) noexcept {
        free(block);
    });

    const size_t shape[2] = {rowCount, columnCount};
    return nb::cast(nb::ndarray<nb::numpy, T, nb::ndim<2>>(data, 2, shape, owner));
}

template <typename T>
const db::types::Embedding::Primitive* getEmbedding(const T& row) {
    if constexpr (isOptional<T>) {
        return row.has_value() ? &*row : nullptr;
    } else {
        return &row;
    }
}

// Embeddings of one dimension, none of them null, become one (rows, dimension) float32
// array. Returns an invalid object when they do not, and the caller falls back to a list.
template <typename T>
nb::object embeddingsAsMatrix(std::span<const T> rows) {
    if (rows.empty() || !getEmbedding(rows.front())) {
        return nb::object();
    }

    const size_t dimension = getEmbedding(rows.front())->size();

    for (const T& row : rows) {
        const db::types::Embedding::Primitive* embedding = getEmbedding(row);
        if (!embedding || embedding->size() != dimension) {
            return nb::object();
        }
    }

    NumpyBuffer<float> values;
    values.resize(rows.size() * dimension);

    float* out = values.data();
    for (const T& row : rows) {
        const db::types::Embedding::Primitive* embedding = getEmbedding(row);
        out = std::copy(embedding->begin(), embedding->end(), out);
    }

    return wrapAsMatrix(values.release(), rows.size(), dimension);
}

// mask is true where the row is null. Both blocks are handed to NumPy, which frees them.
template <typename T>
nb::object wrapAsMaskedArray(T* values, bool* mask, size_t size) {
    const nb::object valueArray = wrapAsNdarray(values, size);
    const nb::object maskArray = wrapAsNdarray(mask, size);

    const nb::object maskedArray = nb::module_::import_("numpy.ma").attr("MaskedArray");
    return maskedArray(valueArray,
                       nb::arg("mask") = maskArray,
                       nb::arg("copy") = false,
                       nb::arg("shrink") = false);
}

template <typename RowToPy>
nb::object buildList(size_t size, const RowToPy& rowToPy) {
    nb::list list = nb::steal<nb::list>(PyList_New(static_cast<Py_ssize_t>(size)));
    if (!list.is_valid()) {
        throw nb::python_error();
    }

    for (size_t row = 0; row < size; row++) {
        PyList_SET_ITEM(list.ptr(), static_cast<Py_ssize_t>(row), rowToPy(row).release().ptr());
    }

    return list;
}

}
