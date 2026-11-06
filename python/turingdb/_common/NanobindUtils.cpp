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
nb::object toPyObject(const T& value, const ValueToPyObject& nested) {
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
        return nested.view(value);
    } else if constexpr (db::IsCell<T>) {
        return nested.element(value);
    } else if constexpr (db::IsNull<T>) {
        return nb::none();
    } else if constexpr (db::IsValueType<T>) {
        return toPyString(db::ValueTypeName::value(value));
    } else if constexpr (db::IsPoint<T>) {
        return nested.point(value);
    } else {
        return toPy(value);
    }
}

// A row's element in a numpy array, or its Python object where its type has no array form.
// A null datetime or duration is numpy's NaT, so a nullable one keeps an array.
template <typename Row>
auto toPyElement(const Row& row, const ValueToPyObject& nested) {
    if constexpr (std::is_arithmetic_v<Row>) {
        return row;
    } else if constexpr (db::IsDateTime<Row> || db::IsDuration<Row>) {
        return temporalMicroseconds(row);
    } else if constexpr (std::is_same_v<Row, db::CustomBool>) {
        return static_cast<uint8_t>(row._boolean);
    } else if constexpr (std::is_same_v<Row, db::ColumnMask::Bool_t>) {
        return static_cast<uint8_t>(row._value);
    } else if constexpr (db::IDLike<Row> && !db::EntityIDLike<Row>) {
        return static_cast<uint64_t>(row.getValue());
    } else if constexpr (db::Hash<Row>) {
        return row.get();
    } else if constexpr (db::Optional<Row>) {
        if (!row) {
            return nb::object(nb::none());
        }

        return toPyObject(*row, nested);
    } else {
        return toPyObject(row, nested);
    }
}

nb::object repeatElement(const nb::object& element, size_t rowCount) {
    nb::list rows;
    for (size_t row = 0; row < rowCount; row++) {
        rows.append(element);
    }
    return rows;
}

template <typename T>
nb::object repeatElement(const T& element, size_t rowCount) {
    return repeatValueAsNdarray(element, rowCount);
}

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

        const auto convert = [col, rowCount, &listVisitor, &value, &dtypeName](const auto* column) {
            using Column = std::decay_t<decltype(*column)>;
            using Row = typename Column::ValueType;
            using Element = decltype(toPyElement(std::declval<const Row&>(), listVisitor));

            db::ColumnTypeGenerator {dtypeName}(column);

            if constexpr (std::is_same_v<Column, db::ColumnConst<Row>>) {
                value = repeatElement(toPyElement(column->getRaw(), listVisitor), rowCount);
            } else if constexpr (std::is_arithmetic_v<Row>) {
                value = wrapVectorAsNdarray(std::move(static_cast<Column*>(col)->getRaw()));
            } else if constexpr (std::is_same_v<Element, nb::object>) {
                nb::list rows;
                for (const Row& row : column->getRaw()) {
                    rows.append(toPyElement(row, listVisitor));
                }
                value = rows;
            } else {
                value = transformVectorAsNdarray<Element>(column->getRaw(), [&listVisitor](const Row& row) {
                    return toPyElement(row, listVisitor);
                });
            }
        };

        db::ColumnSingleDispatcher<db::OutputtedTypes::Allowed, decltype(convert), db::OutputtedTypes::Excluded>::dispatch(col, convert);

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
