#include "IRTestRows.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <type_traits>

#include "columns/ColumnConst.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

#include "ID.h"

using namespace db;
using namespace turing::test;

namespace {

void renderList(const ListView& list, std::string& out);

void renderListElement(const ListElementView& element, std::string& out) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            out += std::to_string(element.getAs<int64_t>());
            return;
        break;

        case ListBufferTypeTag::UInt:
            out += std::to_string(element.getAs<uint64_t>());
            return;
        break;

        case ListBufferTypeTag::Double:
            out += std::to_string(element.getAs<double>());
            return;
        break;

        case ListBufferTypeTag::Bool:
            out += element.getAs<bool>() ? "true" : "false";
            return;
        break;

        case ListBufferTypeTag::String:
            out += element.getAs<std::string_view>();
            return;
        break;

        case ListBufferTypeTag::ListView:
            renderList(element.getAs<ListView>(), out);
            return;
        break;

        case ListBufferTypeTag::Null:
            out += "null";
            return;
        break;

        case ListBufferTypeTag::NodeID:
            out += std::to_string(element.getAs<NodeID>().getValue());
            return;
        break;

        case ListBufferTypeTag::EdgeID:
            out += std::to_string(element.getAs<EdgeID>().getValue());
            return;
        break;

        case ListBufferTypeTag::Embedding:
        case ListBufferTypeTag::INVALID:
        break;
    }

    throw std::runtime_error("IRTestRows: unsupported list element tag");
}

void renderList(const ListView& list, std::string& out) {
    out += '[';

    bool isFirst = true;
    for (const ListElementView& element : list) {
        if (!isFirst) {
            out += ", ";
        }

        isFirst = false;
        renderListElement(element, out);
    }

    out += ']';
}

template <typename T>
void renderValue(const T& value, std::string& out) {
    if constexpr (std::is_same_v<T, std::string_view>) {
        out = std::string(value);
    } else if constexpr (std::is_same_v<T, types::Bool::Primitive>) {
        out = value ? "true" : "false";
    } else {
        out = std::to_string(value);
    }
}

template <typename T>
bool renderValueCell(const Column* column, size_t row, std::string& out) {
    if (const auto* constCol = dynamic_cast<const ColumnConst<T>*>(column)) {
        renderValue<T>(constCol->at(0), out);
        return true;
    }

    if (const auto* plain = dynamic_cast<const ColumnVector<T>*>(column)) {
        renderValue<T>((*plain)[row], out);
        return true;
    }

    const auto* values = dynamic_cast<const ColumnOptVector<T>*>(column);
    if (!values) {
        return false;
    }

    const std::optional<T> value = (*values)[row];
    if (!value) {
        out = "null";
        return true;
    }

    renderValue<T>(*value, out);

    return true;
}

}

void turing::test::renderCell(const Column* column, size_t row, std::string& out) {
    if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
        out = std::to_string((*nodeIDs)[row].getValue());
    } else if (const auto* edgeIDs = dynamic_cast<const ColumnEdgeIDs*>(column)) {
        out = std::to_string((*edgeIDs)[row].getValue());
    } else if (const auto* edgeTypes = dynamic_cast<const ColumnEdgeTypes*>(column)) {
        out = std::to_string((*edgeTypes)[row].getValue());
    } else if (dynamic_cast<const ColumnConst<PropertyNull>*>(column)) {
        out = "null";
    } else if (const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(column)) {
        out.clear();
        renderList((*lists)[row], out);
    } else if (const auto* elements = dynamic_cast<const ColumnVector<ListElementView>*>(column)) {
        out.clear();
        renderListElement((*elements)[row], out);
    } else if (renderValueCell<int64_t>(column, row, out)
               || renderValueCell<uint64_t>(column, row, out)
               || renderValueCell<double>(column, row, out)
               || renderValueCell<types::Bool::Primitive>(column, row, out)
               || renderValueCell<std::string_view>(column, row, out)) {
        // Rendered by the helper for whichever value type matched
    } else {
        throw std::runtime_error("IRTestRows: unsupported output column type");
    }
}

void turing::test::collectPipelineRows(const Dataframe* dataframe, Rows& rows) {
    const Dataframe::NamedColumns& columns = dataframe->cols();
    const size_t rowCount = dataframe->getLogicalRowCount();

    for (size_t row = 0; row < rowCount; row++) {
        Row& cells = rows.emplace_back();
        cells.resize(columns.size());

        for (size_t column = 0; column < columns.size(); column++) {
            renderCell(columns[column]->getColumn(), row, cells[column]);
        }
    }
}

void turing::test::describeRows(const Rows& rows, std::string& out) {
    out.clear();

    for (const Row& row : rows) {
        out += "        {";
        for (size_t cell = 0; cell < row.size(); cell++) {
            out += cell == 0 ? "\"" : ", \"";
            out += row[cell];
            out += "\"";
        }
        out += "},\n";
    }
}

void RowSink::appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) {
    for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
        Row& cells = _rows.emplace_back();
        cells.resize(chunks.size());

        for (size_t column = 0; column < chunks.size(); column++) {
            renderCell(chunks[column], rowIndex, cells[column]);
        }
    }
}

void RowSink::sortedRows(Rows& rows) const {
    rows = _rows;
    std::sort(rows.begin(), rows.end());
}

void CountSink::appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) {
    ASSERT_FALSE(chunks.empty());

    const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks.back());
    ASSERT_NE(counts, nullptr);

    const auto& countRaw = counts->getRaw();
    for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
        _counts.push_back(countRaw[rowIndex]);
    }
}

void CountSink::sortedCounts(Counts& counts) const {
    counts = _counts;
    std::sort(counts.begin(), counts.end());
}

void NullSink::appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) {
}
