#pragma once

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <optional>
#include <utility>
#include <vector>

#include "GraphPath.h"
#include "PathView.h"
#include "ProtoDecodeSink.h"
#include "TuringProtoEncoder.h"
#include "TuringProtoHeaders.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "OptionalTraits.h"

#include "Bitmask.h"

namespace pybindings {

// A column of the Python sink, tagged with its wire type and encoding so the code that hands
// it to Python can recover its concrete type.
class PyColumn {
public:
    PyColumn(net::proto::ColumnInternalKind internalType, net::proto::ColumnKind kind);
    virtual ~PyColumn();

    net::proto::ColumnInternalKind getInternalType() const { return _internalType; }
    net::proto::ColumnKind getKind() const { return _kind; }

private:
    net::proto::ColumnInternalKind _internalType {};
    net::proto::ColumnKind _kind {};
};

template <typename T>
constexpr net::proto::ColumnInternalKind internalTypeOf() {
    return net::proto::ColInternalKindToProtoEnum::map<db::OptionalTraits::unwrap_optional_t<T>>();
}

// The rows of every chunk of a response in one vector. The decoder writes each chunk from
// its row 0, so the column shows it only the chunk being decoded, which starts where the
// rows of the chunks before it end.
template <typename T>
class PyColumnVector : public PyColumn {
public:
    using ValueType = T;
    using DbColumn = db::ColumnVector<T>;

    PyColumnVector()
        : PyColumn(internalTypeOf<T>(), net::proto::ColumnKind::VECTOR)
    {
    }

    size_t size() const { return _rows.size() - _chunkStart; }

    T* data() { return _rows.data() + _chunkStart; }

    T& operator[](size_t row) { return _rows[_chunkStart + row]; }
    const T& operator[](size_t row) const { return _rows[_chunkStart + row]; }

    template <typename... Args>
    T& emplace_back(Args&&... args) { return _rows.emplace_back(std::forward<Args>(args)...); }

    void resize(size_t rowCount) {
        reserve(rowCount);
        _rows.resize(_chunkStart + rowCount);
    }

    // Grows geometrically: a reserve of each chunk's exact size would move the rows of every
    // chunk before it once per chunk.
    void reserve(size_t rowCount) {
        const size_t required = _chunkStart + rowCount;
        if (required > _rows.capacity()) {
            _rows.reserve(std::max(required, 2 * _rows.capacity()));
        }
    }

    // Keeps the rows of the chunk just decoded and starts the next chunk after them
    void endChunk() { _chunkStart = _rows.size(); }

    void clear() {
        _rows.clear();
        _chunkStart = 0;
    }

    std::vector<T>& getRows() { return _rows; }

private:
    std::vector<T> _rows;
    size_t _chunkStart {0};
};

// A path column's rows view IDs the decoder writes into the value store, whose blocks the
// result builder hands to Python with no copy. A null path views no ID.
template <>
class PyColumnVector<db::GraphPath> : public PyColumn {
public:
    using ValueType = db::GraphPath;
    using DbColumn = db::ColumnVector<db::GraphPath>;

    PyColumnVector()
        : PyColumn(internalTypeOf<db::GraphPath>(), net::proto::ColumnKind::VECTOR)
    {
    }

    size_t size() const { return _rows.size(); }

    const db::PathView& operator[](size_t row) const { return _rows[row]; }

    void resize(size_t rowCount) { _rows.resize(rowCount); }

    void setPath(size_t row, db::PathView path) { _rows[row] = path; }

    void clear() { _rows.clear(); }

private:
    std::vector<db::PathView> _rows;
};

// A nullable column, with the null mask pandas reads: 1 for a null row. Its chunks are
// decoded in place as PyColumnVector's are, a fixed-width chunk's validity through
// setValidity once its values are in.
template <typename T>
class PyColumnOptVector : public PyColumn {
public:
    using ValueType = T;
    using DbColumn = db::ColumnOptVector<T>;
    using ValidityMask = DynamicLargeBitMask<uint64_t>;

    PyColumnOptVector()
        : PyColumn(internalTypeOf<T>(), net::proto::ColumnKind::OPTIONAL_VECTOR)
    {
    }

    size_t size() const { return _rows.size() - _chunkStart; }

    bool isValid(size_t row) const { return _nullMask[_chunkStart + row] == 0; }

    void set(size_t row, const T& value) {
        _rows[_chunkStart + row] = value;
        _nullMask[_chunkStart + row] = 0;
    }

    void set(size_t row, T&& value) {
        _rows[_chunkStart + row] = std::move(value);
        _nullMask[_chunkStart + row] = 0;
    }

    // A decoder resumed in a chunk's first row resizes again, so the rows already set stay
    void resize(size_t rowCount) {
        _rows.resize(_chunkStart + rowCount);
        _nullMask.resize(_chunkStart + rowCount, 1);
    }

    T* getChunkValues() { return _rows.data() + _chunkStart; }

    void setValidity(const ValidityMask& validity) {
        uint8_t* const chunkNulls = _nullMask.data() + _chunkStart;
        const size_t rowCount = size();

        for (size_t row = 0; row < rowCount; row++) {
            chunkNulls[row] = validity.test(row) ? 0 : 1;
        }
    }

    // Keeps the rows of the chunk just decoded and starts the next chunk after them
    void endChunk() { _chunkStart = _rows.size(); }

    void clear() {
        _rows.clear();
        _nullMask.clear();
        _chunkStart = 0;
    }

    std::vector<T>& getRows() { return _rows; }
    std::vector<uint8_t>& getNullMask() { return _nullMask; }

private:
    std::vector<T> _rows;
    std::vector<uint8_t> _nullMask;
    size_t _chunkStart {0};
};

// One value standing for every row of a chunk; appendRepeated lays it out for the chunk's
// rows after those of the chunks before it.
template <typename T>
class PyColumnConst : public PyColumn {
public:
    using ValueType = T;
    using DbColumn = db::ColumnConst<T>;

    PyColumnConst()
        : PyColumn(internalTypeOf<T>(),
                   db::OptionalTraits::is_optional_v<T> ? net::proto::ColumnKind::OPTIONAL_CONSTANT
                                                        : net::proto::ColumnKind::CONSTANT)
    {
    }

    const T& getValue() const { return _value; }

    void set(const T& value) { _value = value; }
    void set(T&& value) { _value = std::move(value); }

    void setOptional(const T& value) requires db::OptionalTraits::is_optional_v<T> { _value = value; }
    void setOptional(T&& value) requires db::OptionalTraits::is_optional_v<T> { _value = std::move(value); }

    void appendRepeated(size_t rowCount) { _rows.insert(_rows.end(), rowCount, _value); }

    std::vector<T>& getRows() { return _rows; }

private:
    T _value {};
    std::vector<T> _rows;
};

static_assert(net::proto::VectorColumn<PyColumnVector<uint64_t>, uint64_t>);
static_assert(net::proto::OptionalVectorColumn<PyColumnOptVector<uint64_t>, uint64_t>);
static_assert(net::proto::OptionalBufferColumn<PyColumnOptVector<uint64_t>, uint64_t>);
static_assert(net::proto::ConstColumn<PyColumnConst<uint64_t>, uint64_t>);
static_assert(net::proto::PathViewColumn<PyColumnVector<db::GraphPath>>);
static_assert(net::proto::OptionalConstColumn<PyColumnConst<std::optional<uint64_t>>, uint64_t>);

}
