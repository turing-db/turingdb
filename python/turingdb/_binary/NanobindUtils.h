#pragma once

#include <stdint.h>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>

#include "ID.h"
#include "metadata/PropertyType.h"
#include "columns/Column.h"
#include "columns/ColumnVector.h"

namespace db {
class Dataframe;
class DataframeManager;
class LocalMemory;
class NamedColumn;
}

namespace pybindings {

namespace nb = nanobind;

using PyObj = nb::object;

// Trivial scalar conversions to Python objects. Inline here so the templates
// below can pick them up via overload resolution.
inline PyObj toPy(uint64_t v) {
    return nb::cast(v);
}
inline PyObj toPy(int64_t v) {
    return nb::cast(v);
}
inline PyObj toPy(double v) {
    return nb::cast(v);
}
inline PyObj toPy(db::CustomBool v) {
    return nb::cast(v._boolean);
}
inline PyObj toPy(std::string_view v) {
    return nb::cast(std::string(v));
}
inline PyObj toPy(const std::string& v) {
    return nb::cast(v);
}

template <typename T, int I>
PyObj toPy(const db::ID<T, I>& v) {
    return nb::cast(v.getValue());
}

template <typename T>
PyObj toPy(const std::optional<T>& v) {
    if (!v) {
        return nb::none();
    }
    return toPy(*v);
}

// Bulk-appends one ColumnVector<T>'s contents onto another. Used by the
// streaming query callback to gather chunked dataframes into one buffered
// result. vector::insert with random-access iterators is specialized in the
// stdlib to memcpy for trivially-copyable T, so this works for plain
// numerics, IDs, and std::string alike with no per-call-site special casing.
template <typename T>
void copyColumnVector(const db::Column* col, db::Column* newCol) {
    const auto& castedVec = static_cast<const db::ColumnVector<T>*>(col)->getRaw();
    auto& castedNewVec = static_cast<db::ColumnVector<T>*>(newCol)->getRaw();

    castedNewVec.insert(castedNewVec.end(), castedVec.begin(), castedVec.end());
}

// Moves the source std::vector onto the heap and lets numpy own it. The
// storage pointer (`vec->data()`) stays valid because the heap-allocated
// vector itself isn't destroyed — only its contents were moved out of the
// source. The capsule's deleter destroys the heap vector once Python is done,
// freeing the buffer in one shot. The source vector ends up empty, so the
// column it came from must be considered drained after this call.
template <typename T>
nb::object wrapVectorAsNdarray(std::vector<T>&& src) {
    auto* vec = new std::vector<T>(std::move(src));
    nb::capsule owner(vec, [](void* p) noexcept {
        delete static_cast<std::vector<T>*>(p);
    });
    size_t shape[1] = {vec->size()};
    return nb::cast(nb::ndarray<nb::numpy, T, nb::ndim<1>>(vec->data(), 1, shape, owner));
}

// Materializes a ColumnConst — a single value that logically applies to every
// row — as a fully-expanded numpy array of length `n`. Pandas/numpy have no
// "constant column" type, so we have to spell it out.
template <typename T>
nb::object repeatValueAsNdarray(const T& v, size_t n) {
    std::vector<T> buf(n, v);
    return wrapVectorAsNdarray(std::move(buf));
}

// Like wrapVectorAsNdarray but the source can't be moved directly — we apply
// a per-element conversion into a fresh std::vector<Out>. Used for ID and
// enum columns (NodeID, EdgeID, ValueType, …) where numpy can't store the C++
// type directly and we need to extract the underlying integer with
// `.getValue()` or a static_cast before writing it into the buffer.
template <typename Out, typename In, typename Conv>
nb::object transformVectorAsNdarray(const std::vector<In>& src, Conv conv) {
    std::vector<Out> buf;
    buf.reserve(src.size());
    for (const auto& v : src) {
        buf.push_back(conv(v));
    }
    return wrapVectorAsNdarray(std::move(buf));
}

// Build a Python list by converting each element via toPy(). Used for column
// types that can't fit a typed numpy array — strings (no fixed-width
// representation) and nullable columns (no nullable dtype survives through
// pandas). The toPy(std::optional<T>) overload maps empty optionals to None
// automatically, so a vector<optional<T>> works without a separate overload.
template <typename T>
nb::object vectorAsList(const std::vector<T>& src) {
    nb::list out;
    for (const auto& v : src) {
        out.append(toPy(v));
    }
    return out;
}

// List counterpart to repeatValueAsNdarray: used when the broadcasted value
// is a string, embedding, or other type that can't sit in a typed numpy
// array. We share one Python object across every slot since the value is
// immutable from the caller's point of view.
template <typename T>
nb::object repeatValueAsList(const T& v, size_t n) {
    nb::list out;
    PyObj py = toPy(v);
    for (size_t i = 0; i < n; ++i) {
        out.append(py);
    }
    return out;
}

// Allocates a destination column for every column in incomingDf and registers
// them with bufferedDf. ColumnConst sources have their value copied here once,
// since constants don't get appended to row-by-row.
void allocColumns(const db::Dataframe* incomingDf,
                  db::Dataframe* bufferedDf,
                  db::DataframeManager* dfMan,
                  db::LocalMemory* localMem);

// Runtime dispatcher that appends a single column of unknown kind onto its
// destination. Wraps copyColumnVector with a switch over the column-kind enum.
void addToColumn(const db::Column* col, db::Column* newCol);

// Appends every column of src onto the corresponding column of dst, skipping
// ColumnConst columns (already handled by allocColumns).
void appendDfs(const db::Dataframe* src, db::Dataframe* dst);

// Each embedding is a variable-length float vector, so the natural
// representation is one numpy array per row. The enclosing column ends up as a
// Python list of 1D float arrays rather than a single 2D array — embedding
// dimensions can differ row-to-row.
nb::object embeddingToNdarray(std::span<const float> s);

// Top-level dispatcher: walks every column of the dataframe and picks the
// right converter based on the column's runtime kind. The output is a
// `{"data": {col: ndarray|list}, "dtypes": {col: type_name}}` envelope. The
// dtype names match the HTTP server's vocabulary ("Int64", "UInt64", "Double",
// "Bool", "String") so the Python wrapper can reuse the same dtype_map; ID
// types report their underlying integer ("UInt64" for NodeID/EdgeID/etc.)
// while embeddings and uncommon scalar types report their C++ name and fall
// back to object dtype on the Python side. Anonymous columns get a synthetic
// `$<tag>` name so duplicates don't collide. ColumnVector cases for plain
// numerics drain the source vectors (zero-copy via wrapVectorAsNdarray), so
// the dataframe is single-use after this returns.
nb::dict dataframeToNumpy(db::Dataframe* df);

}
