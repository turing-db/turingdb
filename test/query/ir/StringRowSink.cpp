#include "StringRowSink.h"

#include <algorithm>
#include <optional>
#include <stdexcept>

#include <spdlog/fmt/bundled/format.h>

#include "GraphPath.h"
#include "ID.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"

using namespace db;
using namespace turing::test;

namespace {

template <typename IDType>
bool textOfID(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnVector<IDType>*>(chunk);
    if (!column) {
        return false;
    }

    text = fmt::format("{}", column->getRaw()[rowIndex].getValue());
    return true;
}

template <typename ElementType>
bool textOfPlain(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnVector<ElementType>*>(chunk);
    if (!column) {
        return false;
    }

    text = fmt::format("{}", column->getRaw()[rowIndex]);
    return true;
}

bool textOfValueType(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnVector<ValueType>*>(chunk);
    if (!column) {
        return false;
    }

    text = ValueTypeName::value(column->getRaw()[rowIndex]);
    return true;
}

std::string elementText(const ListElementView& element) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return fmt::format("{}", element.getAs<int64_t>());
        break;

        case ListBufferTypeTag::UInt:
            return fmt::format("{}", element.getAs<uint64_t>());
        break;

        case ListBufferTypeTag::Double:
            return fmt::format("{}", element.getAs<double>());
        break;

        case ListBufferTypeTag::Bool:
            return element.getAs<bool>() ? "true" : "false";
        break;

        case ListBufferTypeTag::String:
            return std::string(element.getAs<std::string_view>());
        break;

        case ListBufferTypeTag::Null:
            return "null";
        break;

        case ListBufferTypeTag::NodeID:
            return fmt::format("{}", element.getAs<NodeID>().getValue());
        break;

        case ListBufferTypeTag::EdgeID:
            return fmt::format("{}", element.getAs<EdgeID>().getValue());
        break;

        case ListBufferTypeTag::Embedding:
        case ListBufferTypeTag::ListView:
        case ListBufferTypeTag::INVALID:
            throw std::runtime_error("StringRowSink cannot read this list element as text");
        break;
    }

    throw std::runtime_error("StringRowSink met an unknown list element tag");
}

// A type-erased column of tagged scalars - the column a heterogeneous UNWIND drives - reads
// each cell through the tag it carries rather than through the column's type.
bool textOfListElement(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnVector<ListElementView>*>(chunk);
    if (!column) {
        return false;
    }

    text = elementText(column->getRaw()[rowIndex]);
    return true;
}

// A list cell reads as its elements joined by ", ", in the order the list holds them.
bool textOfList(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnVector<ListView>*>(chunk);
    if (!column) {
        return false;
    }

    text.clear();
    for (const ListElementView& element : column->getRaw()[rowIndex]) {
        if (!text.empty()) {
            text += ", ";
        }

        text += elementText(element);
    }

    return true;
}

bool textOfPath(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnVector<Path>*>(chunk);
    if (!column) {
        return false;
    }

    text.clear();
    for (const EntityID entity : column->getRaw()[rowIndex]) {
        if (!text.empty()) {
            text += ", ";
        }

        text += fmt::format("{}", entity.getValue());
    }

    return true;
}

template <typename Primitive>
bool textOfOptional(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnOptVector<Primitive>*>(chunk);
    if (!column) {
        return false;
    }

    const std::optional<Primitive>& value = column->getRaw()[rowIndex];
    text = value ? fmt::format("{}", *value) : "null";
    return true;
}

}

StringRowSink::StringRowSink() {
}

StringRowSink::~StringRowSink() {
}

void StringRowSink::setColumnNames(std::span<const std::string_view> names) {
    _names.assign(names.begin(), names.end());
}

void StringRowSink::appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) {
    for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
        Row& row = _rows.emplace_back();
        for (const Column* chunk : chunks) {
            row.push_back(cellText(chunk, rowIndex));
        }
    }
}

void StringRowSink::sortedRows(std::vector<Row>& rows) const {
    rows = _rows;
    std::sort(rows.begin(), rows.end());
}

std::string StringRowSink::cellText(const Column* chunk, size_t rowIndex) {
    std::string text;

    if (textOfID<NodeID>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfID<EdgeID>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfID<LabelID>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfID<PropertyTypeID>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfID<EdgeTypeID>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfValueType(chunk, rowIndex, text)) {
        return text;
    } else if (textOfPlain<uint64_t>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfPlain<int64_t>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfPlain<double>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfPlain<std::string_view>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfPlain<std::string>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptional<int64_t>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptional<uint64_t>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptional<double>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptional<std::string_view>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfListElement(chunk, rowIndex, text)) {
        return text;
    } else if (textOfList(chunk, rowIndex, text)) {
        return text;
    } else if (textOfPath(chunk, rowIndex, text)) {
        return text;
    }

    throw std::runtime_error("StringRowSink cannot read this column kind as text");
}
