#include "NanobindUtils.h"

#include <stdint.h>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "GraphPath.h"
#include "LocalMemory.h"
#include "PyValueConversion.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"
#include "columns/AllowedKinds.h"
#include "columns/Column.h"
#include "columns/ColumnAppend.h"
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

        db::appendColumnRows(srcCol, dstCol, offset, rowCount);
    }
}

nb::object embeddingToNdarray(std::span<const float> s) {
    std::vector<float> buf(s.begin(), s.end());
    return wrapVectorAsNdarray(std::move(buf));
}

nb::object maskedArray(const nb::object& values, std::vector<uint8_t>& nullMask) {
    const nb::object mask = wrapVectorAsNdarrayOf<bool>(std::move(nullMask));
    const nb::object maskedArrayType = nb::module_::import_("numpy.ma").attr("MaskedArray");

    return maskedArrayType(values,
                           nb::arg("mask") = mask,
                           nb::arg("copy") = false,
                           nb::arg("shrink") = false);
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

// A null datetime or duration is numpy's NaT, so a nullable one keeps an array.
template <typename Column>
auto toPyRowElement(const Column* column, size_t row, const ValueToPyObject& nested) {
    using Value = typename Column::UnwrappedType;

    if constexpr (!Column::IS_NULLABLE) {
        return toPyElement((*column)[row], nested);
    } else if constexpr (db::IsDateTime<Value> || db::IsDuration<Value>) {
        if (!column->isValid(row)) {
            return std::numeric_limits<int64_t>::min();
        }

        return temporalMicroseconds(column->getValue(row));
    } else {
        if (!column->isValid(row)) {
            return nb::object(nb::none());
        }

        return toPyObject(column->getValue(row), nested);
    }
}

// A NaN is a value, so a null double is masked rather than read as NaN
template <typename Value>
nb::object nullableFloatArray(db::ColumnOptVector<Value>* column) {
    std::vector<uint8_t> nullMask(column->size());
    for (size_t row = 0; row < nullMask.size(); row++) {
        nullMask[row] = !column->isValid(row);
    }

    return maskedArray(wrapVectorAsNdarray(std::move(column->getValues())), nullMask);
}

template <typename Value>
nb::object nullableFloatArray(const db::ColumnConst<std::optional<Value>>* column, size_t rowCount) {
    const bool isNull = !column->isValid(0);
    std::vector<Value> values(rowCount, isNull ? Value {} : column->getValue(0));
    std::vector<uint8_t> nullMask(rowCount, isNull);

    return maskedArray(wrapVectorAsNdarray(std::move(values)), nullMask);
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
            using Element = decltype(toPyRowElement(column, 0, listVisitor));

            constexpr bool isConst = std::is_same_v<Column, db::ColumnConst<Row>>;
            constexpr bool isNullableFloat = Column::IS_NULLABLE && std::is_floating_point_v<typename Column::UnwrappedType>;

            db::ColumnTypeGenerator {dtypeName}(column);

            if constexpr (isNullableFloat && isConst) {
                value = nullableFloatArray(column, rowCount);
            } else if constexpr (isNullableFloat) {
                value = nullableFloatArray(static_cast<Column*>(col));
            } else if constexpr (isConst) {
                value = repeatElement(toPyRowElement(column, 0, listVisitor), rowCount);
            } else if constexpr (std::is_arithmetic_v<Row>) {
                value = wrapVectorAsNdarray(std::move(static_cast<Column*>(col)->getRaw()));
            } else if constexpr (std::is_same_v<Element, nb::object>) {
                nb::list rows;
                for (size_t row = 0; row < column->size(); row++) {
                    rows.append(toPyRowElement(column, row, listVisitor));
                }
                value = rows;
            } else {
                std::vector<Element> elements(column->size());
                for (size_t row = 0; row < elements.size(); row++) {
                    elements[row] = toPyRowElement(column, row, listVisitor);
                }
                value = wrapVectorAsNdarray(std::move(elements));
            }

            value = withTemporalUnit<Row>(value);
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
