#include "StringRowSink.h"

#include <algorithm>
#include <optional>
#include <stdexcept>

#include <spdlog/fmt/bundled/format.h>

#include "GraphPath.h"
#include "ID.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/PropertyNull.h"
#include "metadata/DateTime.h"
#include "metadata/PropertyType.h"

using namespace db;
using namespace turing::test;

namespace {

// An entity an OPTIONAL MATCH did not match is an invalid ID, which reads as null
template <typename IDType>
bool textOfID(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnVector<IDType>*>(chunk);
    if (!column) {
        return false;
    }

    const IDType id = column->getRaw()[rowIndex];
    text = id.isValid() ? fmt::format("{}", id.getValue()) : "null";

    return true;
}

// A nullable entity column, which is what a procedure declaring a nullable NODE return
// value writes: an absent row reads as null, as an invalid ID does
template <typename IDType>
bool textOfOptionalID(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnOptVector<IDType>*>(chunk);
    if (!column) {
        return false;
    }

    const std::optional<IDType>& id = column->getRaw()[rowIndex];
    const bool present = id.has_value() && id->isValid();
    text = present ? fmt::format("{}", id->getValue()) : "null";

    return true;
}

// A constant column holds the one value every row of the relation reads, so the row index
// says nothing about which value to read
template <typename ElementType>
bool textOfConstant(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnConst<ElementType>*>(chunk);
    if (!column) {
        return false;
    }

    text = fmt::format("{}", column->at(rowIndex));
    return true;
}

// The untyped null constant, which the null literal and a read of a name no property
// carries both compile to: every row is an absent value of no type at all
bool textOfNullConstant(const Column* chunk, std::string& text) {
    if (!dynamic_cast<const ColumnConst<PropertyNull>*>(chunk)) {
        return false;
    }

    text = "null";
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

// A datetime renders as the ISO-8601 instant it names rather than as its count, which is
// what every other reader of one shows
bool textOfDateTime(const Column* chunk, size_t rowIndex, std::string& text) {
    if (const auto* column = dynamic_cast<const ColumnVector<DateTime>*>(chunk)) {
        DateTime::format(text, column->getRaw()[rowIndex]);
        return true;
    }

    const auto* optional = dynamic_cast<const ColumnOptVector<DateTime>*>(chunk);
    if (!optional) {
        return false;
    }

    const std::optional<DateTime>& value = optional->getRaw()[rowIndex];
    if (!value) {
        text = "null";
        return true;
    }

    DateTime::format(text, *value);
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

        case ListBufferTypeTag::DateTime: {
            std::string formatted;
            DateTime::format(formatted, element.getAs<types::DateTime::Primitive>());

            return formatted;
        }
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

// The nullable sibling of textOfListElement: what an index into a list naming no one type
// produces, absent where the position held no element.
bool textOfOptionalListElement(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnOptVector<ListElementView>*>(chunk);
    if (!column) {
        return false;
    }

    const std::optional<ListElementView>& element = column->getRaw()[rowIndex];
    text = element ? elementText(*element) : "null";

    return true;
}

// The same index over literal operands alone, which stays a constant.
bool textOfConstOptionalListElement(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnConst<std::optional<ListElementView>>*>(chunk);
    if (!column) {
        return false;
    }

    const std::optional<ListElementView>& element = (*column)[rowIndex];
    text = element ? elementText(*element) : "null";

    return true;
}

// A list cell reads as its elements joined by ", ", in the order the list holds them.
void appendListText(const ListView& list, std::string& text) {
    for (const ListElementView& element : list) {
        if (!text.empty()) {
            text += ", ";
        }

        text += elementText(element);
    }
}

bool textOfList(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnVector<ListView>*>(chunk);
    if (!column) {
        return false;
    }

    text.clear();
    appendListText(column->getRaw()[rowIndex], text);

    return true;
}

// The labels of a node the query created fold to a constant, which holds the one list
// every row of the projection reads
bool textOfConstList(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnConst<ListView>*>(chunk);
    if (!column) {
        return false;
    }

    text.clear();
    appendListText(column->at(0), text);

    return true;
}

bool textOfOptionalList(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnOptVector<ListView>*>(chunk);
    if (!column) {
        return false;
    }

    const std::optional<ListView>& list = column->getRaw()[rowIndex];

    text.clear();
    if (list.has_value()) {
        appendListText(*list, text);
    } else {
        text = "null";
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

std::string boolText(const std::optional<CustomBool>& value) {
    if (!value) {
        return "null";
    }

    return *value ? "true" : "false";
}

// A nullable boolean, which a three-valued predicate such as IN produces
bool textOfOptionalBool(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnOptVector<CustomBool>*>(chunk);
    if (!column) {
        return false;
    }

    text = boolText(column->getRaw()[rowIndex]);
    return true;
}

// The same predicate over constant operands alone, which stays a constant: it holds the
// one value every row of the projection reads.
bool textOfConstOptionalBool(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnConst<std::optional<CustomBool>>*>(chunk);
    if (!column) {
        return false;
    }

    text = boolText(column->at(rowIndex));
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

// An expression over constants alone is computed once and reaches the sink as the single
// value it stands for, in a ColumnConst rather than in a column of rows.
template <typename ElementType>
bool textOfConst(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnConst<ElementType>*>(chunk);
    if (!column) {
        return false;
    }

    text = fmt::format("{}", (*column)[rowIndex]);
    return true;
}

template <typename Primitive>
bool textOfOptionalConst(const Column* chunk, size_t rowIndex, std::string& text) {
    const auto* column = dynamic_cast<const ColumnConst<std::optional<Primitive>>*>(chunk);
    if (!column) {
        return false;
    }

    const std::optional<Primitive>& value = (*column)[rowIndex];
    text = value ? fmt::format("{}", *value) : "null";
    return true;
}

}

StringRowSink::StringRowSink() {
}

StringRowSink::~StringRowSink() {
}

void StringRowSink::declareOutput(std::span<const std::string_view> names,
                                  std::span<const Column* const> chunks) {
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
    } else if (textOfDateTime(chunk, rowIndex, text)) {
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
    } else if (textOfConstant<int64_t>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfConstant<uint64_t>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfConstant<double>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfConstant<std::string_view>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfConstant<std::string>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfNullConstant(chunk, text)) {
        return text;
    } else if (textOfOptional<int64_t>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptional<uint64_t>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptional<double>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptional<std::string_view>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptional<std::string>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptionalID<NodeID>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptionalID<EdgeID>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptionalBool(chunk, rowIndex, text)) {
        return text;
    } else if (textOfConstOptionalBool(chunk, rowIndex, text)) {
        return text;
    } else if (textOfConst<int64_t>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfConst<uint64_t>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfConst<double>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfConst<std::string_view>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptionalConst<int64_t>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptionalConst<uint64_t>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptionalConst<double>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptionalConst<std::string_view>(chunk, rowIndex, text)) {
        return text;
    } else if (textOfListElement(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptionalListElement(chunk, rowIndex, text)) {
        return text;
    } else if (textOfConstOptionalListElement(chunk, rowIndex, text)) {
        return text;
    } else if (textOfList(chunk, rowIndex, text)) {
        return text;
    } else if (textOfConstList(chunk, rowIndex, text)) {
        return text;
    } else if (textOfOptionalList(chunk, rowIndex, text)) {
        return text;
    } else if (textOfPath(chunk, rowIndex, text)) {
        return text;
    }

    throw std::runtime_error("StringRowSink cannot read this column kind as text");
}
