#include "NanobindUtils.h"

#include <stdint.h>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "GraphPath.h"
#include "LocalMemory.h"
#include "PyTimeTypes.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"
#include "columns/AllowedKinds.h"
#include "columns/Column.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ContainerKind.h"
#include "CellView.h"
#include "CellUtils.h"
#include "list/ListView.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "OutputValues.h"

#include "TuringException.h"

namespace pybindings {

void allocColumns(const db::Dataframe* incomingDf,
                  db::Dataframe* bufferedDf,
                  db::DataframeManager* dfMan,
                  db::LocalMemory* localMem,
                  std::vector<std::string>* nameStorage) {
    const auto& srcCols = incomingDf->cols();
    nameStorage->reserve(srcCols.size());
    for (const db::NamedColumn* namedCol : srcCols) {
        const db::Column* srcCol = namedCol->getColumn();
        db::Column* newCol = localMem->allocSame(srcCol);
        db::NamedColumn* newNamedCol = db::NamedColumn::create(dfMan, newCol, dfMan->allocTag());
        // Copy the name into our own storage; the source view points into the
        // chunk buffer which is reused for subsequent chunks.
        nameStorage->emplace_back(namedCol->getName());
        newNamedCol->rename(nameStorage->back());
        bufferedDf->addColumn(newNamedCol);

        // We just need to copy the value over at alloc time for constants
        if (srcCol->getContainerKind() == db::ContainerKind::code<db::ColumnConst>()) {
            newCol->assign(srcCol);
        }
    }
}

void addToColumn(const db::Column* col, db::Column* newCol, size_t offset, size_t rowCount) {
    // ColumnConst columns are handled once in allocColumns via assign() and
    // skipped in appendDfs, so only ColumnVector kinds reach here. Const/Set/Mask
    // containers are excluded from the dispatcher below, so any non-vector column
    // hits the dispatcher's unsupported() path and throws a FatalException — an
    // unsupported column type still produces a clear failure.
    const auto appendCol = [newCol, offset, rowCount](const auto* typedCol) {
        using T = typename std::decay_t<decltype(*typedCol)>::ValueType;
        copyColumnVector<T>(typedCol, newCol, offset, rowCount);
    };

    using ExcludedNonVector = db::ExcludedContainers<
        db::ContainerKind::code<db::ColumnConst>(),
        db::ContainerKind::code<db::ColumnSet>(),
        db::ContainerKind::code<db::ColumnMask>()>;
    using Dispatcher = db::ColumnSingleDispatcher<db::OutputtedTypes::Allowed,
                                                  decltype(appendCol),
                                                  ExcludedNonVector>;

    Dispatcher::dispatch(col, appendCol);
}

void appendDfs(const db::Dataframe* src, db::Dataframe* dst) {
    const auto& srcCols = src->cols();
    const auto& dstCols = dst->cols();

    // A chunk of constants alone grows no column, so the running total has to be carried
    // rather than read back off the buffered columns.
    dst->setDeclaredRowCount(dst->getDeclaredRowCount() + src->getLogicalRowCount());

    for (size_t i = 0; i < srcCols.size(); ++i) {
        db::Column* dstCol = dstCols[i]->getColumn();

        // ColumnConsts are copied once - at column alloc time
        if (dstCol->getContainerKind() == db::ContainerKind::code<db::ColumnConst>()) {
            continue;
        }

        const db::Column* srcCol = srcCols[i]->getColumn();
        addToColumn(srcCol, dstCol, 0, srcCol->size());
    }
}

void allocChunkColumns(std::span<const std::string_view> names,
                       std::span<const db::Column* const> chunks,
                       db::Dataframe* bufferedDf,
                       db::DataframeManager* dfMan,
                       db::LocalMemory* localMem,
                       std::vector<std::string>* nameStorage) {
    nameStorage->reserve(chunks.size());

    for (size_t columnIndex = 0; columnIndex < chunks.size(); columnIndex++) {
        db::Column* newCol = localMem->allocSame(chunks[columnIndex]);
        db::NamedColumn* newNamedCol = db::NamedColumn::create(dfMan, newCol, dfMan->allocTag());

        nameStorage->emplace_back(columnIndex < names.size() ? names[columnIndex] : std::string_view {});
        newNamedCol->rename(nameStorage->back());

        bufferedDf->addColumn(newNamedCol);
    }
}

void appendChunkColumns(std::span<const db::Column* const> chunks,
                        size_t offset,
                        size_t rowCount,
                        db::Dataframe* dst) {
    const auto& dstCols = dst->cols();

    dst->setDeclaredRowCount(dst->getDeclaredRowCount() + rowCount);

    for (size_t columnIndex = 0; columnIndex < chunks.size(); columnIndex++) {
        const db::Column* srcCol = chunks[columnIndex];
        db::Column* dstCol = dstCols[columnIndex]->getColumn();

        // A constant stands for every row, so it is copied whole rather than windowed
        if (dstCol->getContainerKind() == db::ContainerKind::code<db::ColumnConst>()) {
            dstCol->assign(srcCol);
            continue;
        }

        addToColumn(srcCol, dstCol, offset, rowCount);
    }
}

template <typename T>
int64_t temporalMicroseconds(T value) {
    return value.getMicroseconds();
}

template <typename T>
int64_t temporalMicroseconds(const std::optional<T>& value) {
    if (!value.has_value()) {
        return std::numeric_limits<int64_t>::min();
    }

    return value->getMicroseconds();
}

template <typename T>
nb::object temporalColumnAsNdarray(const std::vector<T>& src) {
    return transformVectorAsNdarray<int64_t>(src, [](const T& value) {
        return temporalMicroseconds(value);
    });
}

nb::object embeddingToNdarray(std::span<const float> s) {
    std::vector<float> buf(s.begin(), s.end());
    return wrapVectorAsNdarray(std::move(buf));
}

nb::str toPyString(std::string_view text) {
    PyObject* const decoded = PyUnicode_DecodeUTF8(text.data(), static_cast<Py_ssize_t>(text.size()), "replace");
    if (!decoded) {
        throw nb::python_error();
    }

    return nb::steal<nb::str>(decoded);
}

void setRuntimeError(std::string_view message) {
    const nb::str text = toPyString(message);
    PyErr_SetObject(PyExc_RuntimeError, text.ptr());
}

namespace {

// Converts a list or map value (or a single list element / map entry) to plain Python
// objects — scalars, a list of floats for an embedding, nested lists, and dicts for maps.
// Deliberately plain objects, not ndarrays, so the result compares equal to the JSON-parsed
// expectation. Nesting recurses through view() -> element()/entry() -> operator(); keeping
// them as members of one struct lets them call each other without a forward declaration.
struct ValueToPyObject {
    mutable PyTimeTypes _times;
    mutable nb::object _point;

    nb::object duration(db::Duration value) const {
        return _times.getTimedelta()(nb::arg("microseconds") = value.getMicroseconds());
    }

    nb::object dateTime(db::DateTime value) const {
        return _times.getEpoch() + duration(db::Duration {value.getMicroseconds()});
    }

    nb::object point(const db::Point& value) const {
        if (!_point.is_valid()) {
            _point = nb::module_::import_("turingdb.point").attr("Point");
        }

        const db::PointCRS crs = value.getCRS();
        const nb::object z = db::Point::getDimension(crs) == 3 ? nb::cast(value.getZ()) : nb::none();

        return _point(db::Point::getSRID(crs), value.getX(), value.getY(), z);
    }

    nb::object view(const db::ListView& listView) const {
        nb::list out;
        for (const db::CellView element : listView.elements()) {
            out.append(this->element(element));
        }
        return out;
    }

    nb::object element(const db::CellView element) const {
        return db::CellTagDispatcher {element.getTag()}.execute(*this, element);
    }

    nb::object view(const db::MapView& mapView) const {
        nb::dict out;
        for (const db::MapEntryView entry : mapView.entries()) {
            out[toPyString(entry.getKey())] = this->entry(entry);
        }
        return out;
    }

    nb::object entry(const db::MapEntryView entry) const {
        return element(entry.getValue());
    }

    template <typename T>
    nb::object operator()(const db::CellView element) const {
        if constexpr (std::is_same_v<T, db::types::Embedding::Primitive>) {
            nb::list floats;
            for (const float value : element.getAs<T>()) {
                floats.append(nb::cast(value));
            }
            return floats;
        } else if constexpr (std::is_same_v<T, db::ListView>) {
            return view(element.getAs<T>());
        } else if constexpr (std::is_same_v<T, db::MapView>) {
            return view(element.getAs<T>());
        } else if constexpr (std::is_same_v<T, db::PropertyNull>) {
            return nb::none();
        } else if constexpr (std::is_same_v<T, db::types::Duration::Primitive>) {
            return duration(element.getAs<T>());
        } else if constexpr (std::is_same_v<T, db::types::DateTime::Primitive>) {
            return dateTime(element.getAs<T>());
        } else if constexpr (std::is_same_v<T, db::types::Point::Primitive>) {
            return point(element.getAs<T>());
        } else if constexpr (std::is_same_v<T, db::PathView>) {
            nb::list ids;
            for (const uint64_t id : element.getAs<T>().getIDs()) {
                ids.append(nb::cast(id));
            }
            return ids;
        } else {
            return toPy(element.getAs<T>());
        }
    }

};

template <typename T>
concept HasNullSentinel = db::IsDateTime<T> || db::IsDuration<T>;

template <typename T>
concept HasArrayForm = HasNullSentinel<T>
                    || db::IsBool<T>
                    || (db::IDLike<T> && !db::EntityIDLike<T>)
                    || db::Hash<T>
                    || std::is_arithmetic_v<T>;

template <HasArrayForm T>
auto arrayValue(const T& value) {
    if constexpr (HasNullSentinel<T>) {
        return temporalMicroseconds(value);
    } else if constexpr (db::IsBool<T>) {
        return static_cast<uint8_t>(value._boolean ? 1 : 0);
    } else if constexpr (db::IDLike<T>) {
        return static_cast<uint64_t>(value.getValue());
    } else if constexpr (db::Hash<T>) {
        return value.get();
    } else {
        return value;
    }
}

template <HasArrayForm T>
using ArrayValue = decltype(arrayValue(std::declval<const T&>()));

// A null datetime or duration is numpy's NaT, so a nullable one stays an array where any
// other nullable column becomes a list holding None.
struct ColumnToPyObject {
    db::Column* _column {nullptr};
    size_t _rowCount {0};
    const ValueToPyObject& _values;
    nb::object& _value;
    std::string& _dtypeName;

    ColumnToPyObject(db::Column* column,
                     size_t rowCount,
                     const ValueToPyObject& values,
                     nb::object& value,
                     std::string& dtypeName)
        : _column(column),
        _rowCount(rowCount),
        _values(values),
        _value(value),
        _dtypeName(dtypeName)
    {
    }

    template <typename T>
    void operator()(const db::ColumnVector<T>* column) {
        db::ColumnTypeGenerator {_dtypeName}(column);

        if constexpr (std::is_arithmetic_v<T>) {
            _value = wrapVectorAsNdarray(std::move(static_cast<db::ColumnVector<T>*>(_column)->getRaw()));
        } else if constexpr (HasArrayForm<T>) {
            _value = transformVectorAsNdarray<ArrayValue<T>>(column->getRaw(), arrayValue<T>);
        } else {
            nb::list rows;
            for (const T& value : column->getRaw()) {
                rows.append(object(value));
            }
            _value = rows;
        }
    }

    template <typename T>
    void operator()(const db::ColumnVector<std::optional<T>>* column) {
        db::ColumnTypeGenerator {_dtypeName}(column);

        if constexpr (HasNullSentinel<T>) {
            _value = temporalColumnAsNdarray(column->getRaw());
        } else {
            nb::list rows;
            for (const std::optional<T>& value : column->getRaw()) {
                if (value) {
                    rows.append(object(*value));
                } else {
                    rows.append(nb::none());
                }
            }
            _value = rows;
        }
    }

    template <typename T>
    void operator()(const db::ColumnConst<T>* column) {
        db::ColumnTypeGenerator {_dtypeName}(column);
        const T& value = column->getRaw();

        if constexpr (HasArrayForm<T>) {
            _value = repeatValueAsNdarray(arrayValue(value), _rowCount);
        } else {
            _value = repeatObject(object(value));
        }
    }

    template <typename T>
    void operator()(const db::ColumnConst<std::optional<T>>* column) {
        db::ColumnTypeGenerator {_dtypeName}(column);
        const std::optional<T>& value = column->getRaw();

        if constexpr (HasNullSentinel<T>) {
            _value = repeatValueAsNdarray(temporalMicroseconds(value), _rowCount);
        } else if (value) {
            _value = repeatObject(object(*value));
        } else {
            _value = repeatObject(nb::none());
        }
    }

    void operator()(const db::ColumnMask* column) {
        db::ColumnTypeGenerator {_dtypeName}(column);
        _value = transformVectorAsNdarray<uint8_t>(column->getRaw(), [](const db::ColumnMask::Bool_t& value) {
            return static_cast<uint8_t>(value._value ? 1 : 0);
        });
    }

    template <typename T>
    nb::object object(const T& value) const {
        if constexpr (db::IsEmbedding<T>) {
            return embeddingToNdarray(value);
        } else if constexpr (db::IsPath<T>) {
            if (value.isNull()) {
                return nb::none();
            }

            nb::list ids;
            for (const uint64_t id : value.getIDs()) {
                ids.append(nb::cast(id));
            }
            return ids;
        } else if constexpr (db::IsListView<T> || db::IsMap<T>) {
            return _values.view(value);
        } else if constexpr (db::IsCell<T>) {
            return _values.element(value);
        } else if constexpr (db::IsNull<T>) {
            return nb::none();
        } else if constexpr (db::IsValueType<T>) {
            return toPyString(db::ValueTypeName::value(value));
        } else if constexpr (db::IsDateTime<T>) {
            return _values.dateTime(value);
        } else if constexpr (db::IsDuration<T>) {
            return _values.duration(value);
        } else if constexpr (db::IsPoint<T>) {
            return _values.point(value);
        } else if constexpr (db::Hash<T>) {
            return nb::cast(value.get());
        } else if constexpr (std::is_arithmetic_v<T>) {
            return nb::cast(value);
        } else {
            return toPy(value);
        }
    }

    nb::object repeatObject(const nb::object& value) const {
        nb::list rows;
        for (size_t row = 0; row < _rowCount; row++) {
            rows.append(value);
        }
        return rows;
    }
};

}

nb::dict dataframeToNumpy(db::Dataframe* df) {
    nb::dict data;
    nb::dict dtypes;
    ValueToPyObject listVisitor;

    const size_t rowCount = df->getLogicalRowCount();

    for (const db::NamedColumn* namedCol : df->cols()) {
        const std::string_view name = namedCol->getName();
        const std::string keyStr =
            name.empty() ? "$" + std::to_string(namedCol->getTag().getValue())
                         : std::string(name);

        db::Column* col = namedCol->getColumn();
        nb::object value;
        std::string dtypeName;

        ColumnToPyObject converter(col, rowCount, listVisitor, value, dtypeName);
        db::ColumnSingleDispatcher<db::OutputtedTypes::Allowed, ColumnToPyObject, db::OutputtedTypes::Excluded>::dispatch(col, converter);

        const nb::object key = toPyString(keyStr);
        data[key] = value;
        dtypes[key] = nb::cast(dtypeName);
    }

    nb::dict out;
    out["data"] = data;
    out["dtypes"] = dtypes;
    return out;
}

}
