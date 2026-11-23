#include "PyResultBuilder.h"

#include <stddef.h>
#include <stdint.h>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "NanobindUtils.h"
#include "OptionalTraits.h"
#include "OutputValues.h"
#include "PyColumn.h"
#include "PyColumnContainer.h"
#include "PyObjectColumn.h"
#include "PySink.h"
#include "PyValueStore.h"
#include "PyValueConversion.h"
#include "TuringProtoDecoder.h"
#include "TuringProtoDecoderConcepts.h"
#include "TuringProtoHeaders.h"

#include "TuringException.h"

using namespace pybindings;

namespace {

using ColumnKind = net::proto::ColumnKind;

// A value numpy holds in an array, each value's bytes read as its element
template <typename T>
concept NumpyValue = std::is_arithmetic_v<T>
                  || std::is_same_v<T, db::CustomBool>
                  || db::IsDateTime<T>
                  || db::IsDuration<T>
                  || db::IDLike<T>
                  || db::Hash<T>;

template <typename T>
concept Temporal = db::IsDateTime<T> || db::IsDuration<T>;

template <typename T>
auto numpyElementOf() {
    if constexpr (std::is_same_v<T, db::CustomBool>) {
        return uint8_t {};
    } else if constexpr (Temporal<T>) {
        return int64_t {};
    } else if constexpr (db::IDLike<T> || db::Hash<T>) {
        return db::UnsignedElement<T> {};
    } else {
        return T {};
    }
}

template <typename T>
using NumpyElement = decltype(numpyElementOf<T>());

[[noreturn]] void throwUnsupportedColumn() {
    throw TuringException("Unsupported column in the result");
}

template <typename T>
nb::object wrapRows(std::vector<T>& rows) {
    return withTemporalUnit<T>(wrapVectorAsNdarrayOf<NumpyElement<T>>(std::move(rows)));
}

// An invalid ID reads as None, as a null one does
template <typename T>
void maskInvalidIDs(const std::vector<T>& rows, std::vector<uint8_t>& nullMask) {
    for (size_t row = 0; row < rows.size(); row++) {
        if (!rows[row].isValid()) {
            nullMask[row] = 1;
        }
    }
}

template <typename T>
nb::object nonNullArray(std::vector<T>& rows) {
    if constexpr (db::EntityIDLike<T>) {
        std::vector<uint8_t> nullMask(rows.size(), 0);
        maskInvalidIDs(rows, nullMask);
        return maskedArray(wrapRows(rows), nullMask);
    } else {
        return wrapRows(rows);
    }
}

// A null datetime or duration is numpy's NaT, the smallest int64, so it needs no mask
template <typename T>
nb::object nullableArray(std::vector<T>& rows, std::vector<uint8_t>& nullMask) {
    if constexpr (Temporal<T>) {
        for (size_t row = 0; row < rows.size(); row++) {
            if (nullMask[row] != 0) {
                rows[row] = T {std::numeric_limits<int64_t>::min()};
            }
        }
        return wrapRows(rows);
    } else {
        if constexpr (db::IDLike<T>) {
            maskInvalidIDs(rows, nullMask);
        }
        return maskedArray(wrapRows(rows), nullMask);
    }
}

template <typename T>
nb::object nullableConstArray(std::vector<std::optional<T>>& rows) {
    std::vector<T> values(rows.size());
    std::vector<uint8_t> nullMask(rows.size(), 1);

    for (size_t row = 0; row < rows.size(); row++) {
        if (rows[row].has_value()) {
            values[row] = *rows[row];
            nullMask[row] = 0;
        }
    }

    return nullableArray(values, nullMask);
}

template <typename T>
nb::object constObject(const T& value, const ValueToPyObject& converter) {
    if constexpr (db::OptionalTraits::is_optional_v<T>) {
        if (!value.has_value()) {
            return nb::none();
        }

        return toPyObject(*value, converter);
    } else {
        return toPyObject(value, converter);
    }
}

// Calls fn with the column cast to its concrete type, for element T in encoding kind
template <typename T, typename Fn>
void visitColumn(PyColumn* column, ColumnKind kind, Fn&& fn) {
    switch (kind) {
        case ColumnKind::VECTOR: {
            if constexpr (net::proto::SupportedColumnVectorTypes<T, PySink>) {
                return fn(static_cast<PyColumnVector<T>*>(column));
            }
        }
        break;
        case ColumnKind::OPTIONAL_VECTOR: {
            if constexpr (net::proto::SupportedColumnOptVectorTypes<T, PySink>) {
                return fn(static_cast<PyColumnOptVector<T>*>(column));
            }
        }
        break;
        case ColumnKind::CONSTANT: {
            if constexpr (net::proto::SupportedColumnConstTypes<T, PySink>) {
                return fn(static_cast<PyColumnConst<T>*>(column));
            }
        }
        break;
        case ColumnKind::OPTIONAL_CONSTANT: {
            if constexpr (net::proto::SupportedColumnOptConstTypes<T, PySink>) {
                return fn(static_cast<PyColumnConst<std::optional<T>>*>(column));
            }
        }
        break;
    }

    throwUnsupportedColumn();
}

template <typename Column>
concept NumpyColumn = NumpyValue<db::OptionalTraits::unwrap_optional_t<typename Column::ValueType>>;

template <typename Column>
concept RepeatedColumn = std::is_same_v<Column, PyColumnConst<typename Column::ValueType>>;

// Keeps the chunk of each number column, and finds whether any column reads as objects
struct AppendNumbersFn {
    PyColumn* _column {nullptr};
    size_t _rowCount {0};
    bool* _hasObjects {nullptr};

    template <typename T>
    void operator()(ColumnKind kind) const {
        visitColumn<T>(_column, kind, [&](auto* column) {
            appendNumbers(column);
        });
    }

    template <typename Column>
    void appendNumbers(Column* column) const {
        if constexpr (!NumpyColumn<Column>) {
            *_hasObjects = true;
        } else if constexpr (RepeatedColumn<Column>) {
            column->appendRepeated(_rowCount);
        } else {
            column->endChunk();
        }
    }
};

struct AppendObjectsFn {
    PyColumn* _column {nullptr};
    PyObjectColumn* _objects {nullptr};
    size_t _rowCount {0};
    const ValueToPyObject* _converter {nullptr};

    template <typename T>
    void operator()(ColumnKind kind) const {
        visitColumn<T>(_column, kind, [&](auto* column) {
            if constexpr (!NumpyColumn<std::remove_pointer_t<decltype(column)>>) {
                appendObjects(column);
            }
        });
    }

    template <typename T>
    void appendObjects(PyColumnVector<T>* column) const {
        const std::span<PyObject*> block = _objects->addBlock(column->size());

        for (size_t row = 0; row < block.size(); row++) {
            block[row] = toPyObject((*column)[row], *_converter).release().ptr();
        }
        column->clear();
    }

    template <typename T>
    void appendObjects(PyColumnOptVector<T>* column) const {
        const T* const values = column->getChunkValues();
        const std::span<PyObject*> block = _objects->addBlock(column->size());

        for (size_t row = 0; row < block.size(); row++) {
            if (column->isValid(row)) {
                block[row] = toPyObject(values[row], *_converter).release().ptr();
            } else {
                block[row] = nb::none().release().ptr();
            }
        }
        column->clear();
    }

    template <typename T>
    void appendObjects(PyColumnConst<T>* column) const {
        const nb::object value = constObject(column->getValue(), *_converter);
        const std::span<PyObject*> block = _objects->addBlock(_rowCount);

        for (size_t row = 0; row < block.size(); row++) {
            block[row] = value.inc_ref().ptr();
        }
    }
};

template <typename T>
nb::object numpyArray(PyColumnVector<T>* column) {
    return nonNullArray(column->getRows());
}

template <typename T>
nb::object numpyArray(PyColumnOptVector<T>* column) {
    return nullableArray(column->getRows(), column->getNullMask());
}

template <typename T>
nb::object numpyArray(PyColumnConst<T>* column) {
    return nonNullArray(column->getRows());
}

template <typename T>
nb::object numpyArray(PyColumnConst<std::optional<T>>* column) {
    return nullableConstArray(column->getRows());
}

struct BuildColumnFn {
    PyColumn* _column {nullptr};
    PyObjectColumn* _objects {nullptr};
    nb::object* _value {nullptr};
    std::string* _dtypeName {nullptr};

    template <typename T>
    void operator()(ColumnKind kind) const {
        visitColumn<T>(_column, kind, [&](auto* column) {
            db::ColumnTypeGenerator {*_dtypeName}(column);

            if constexpr (NumpyColumn<std::remove_pointer_t<decltype(column)>>) {
                *_value = numpyArray(column);
            } else {
                *_value = _objects->toArray();
            }
        });
    }
};

}

PyResultBuilder::PyResultBuilder(PyColumnContainer* container, PyValueStore* values)
    : _container(container),
    _values(values)
{
}

PyResultBuilder::~PyResultBuilder() {
}

void PyResultBuilder::appendChunk() {
    const size_t columnCount = _container->size();
    const size_t rowCount = _container->getRowCount();

    bool hasObjects = false;
    for (size_t index = 0; index < columnCount; index++) {
        PyColumn* column = (*_container)[index];

        const AppendNumbersFn appendNumbers {
            ._column = column,
            ._rowCount = rowCount,
            ._hasObjects = &hasObjects,
        };

        net::proto::TuringProtoDecoder<PySink>::dispatchColumnType(column->getInternalType(), column->getKind(), appendNumbers);
    }

    if (!hasObjects) {
        return;
    }

    nb::gil_scoped_acquire acquire;
    ValueToPyObject converter;

    std::vector<std::vector<uint64_t>> pathBlocks;
    _values->takePathIDs(pathBlocks);
    converter.adoptPathIDs(pathBlocks);

    allocObjectColumns();

    for (size_t index = 0; index < columnCount; index++) {
        PyColumn* column = (*_container)[index];

        const AppendObjectsFn appendObjects {
            ._column = column,
            ._objects = _objectColumns[index].get(),
            ._rowCount = rowCount,
            ._converter = &converter,
        };

        net::proto::TuringProtoDecoder<PySink>::dispatchColumnType(column->getInternalType(), column->getKind(), appendObjects);
    }
}

void PyResultBuilder::build(nb::dict& envelope) {
    const size_t columnCount = _container->size();
    allocObjectColumns();

    nb::dict data;
    nb::dict dtypes;

    for (size_t index = 0; index < columnCount; index++) {
        PyColumn* column = (*_container)[index];
        const std::string& name = _container->getName(index);
        const std::string key = name.empty() ? "$" + std::to_string(index) : name;

        nb::object value;
        std::string dtypeName;

        const BuildColumnFn buildColumn {
            ._column = column,
            ._objects = _objectColumns[index].get(),
            ._value = &value,
            ._dtypeName = &dtypeName,
        };

        net::proto::TuringProtoDecoder<PySink>::dispatchColumnType(column->getInternalType(), column->getKind(), buildColumn);

        const nb::object pyKey = toPyString(key);
        data[pyKey] = value;
        dtypes[pyKey] = nb::cast(dtypeName);
    }

    envelope["data"] = data;
    envelope["dtypes"] = dtypes;
}

void PyResultBuilder::allocObjectColumns() {
    const size_t columnCount = _container->size();

    while (_objectColumns.size() < columnCount) {
        _objectColumns.push_back(std::make_unique<PyObjectColumn>());
    }
}
