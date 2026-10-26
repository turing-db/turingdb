#include "StringRowSink.h"

#include <math.h>
#include <algorithm>
#include <concepts>
#include <iterator>
#include <optional>
#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "CellUtils.h"
#include "CellView.h"
#include "GraphPath.h"
#include "ID.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnVector.h"
#include "list/ListView.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"
#include "metadata/DateTime.h"
#include "metadata/Duration.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitHash.h"

using namespace db;
using namespace turing::test;

namespace {

void appendValueText(CellView cell, std::string& text);
void appendValueText(const ListView& list, std::string& text);
void appendValueText(const MapView& map, std::string& text);
void appendValueText(types::Embedding::Primitive embedding, std::string& text);

// x86 and ARM give the NaN of sqrt(-1) opposite sign bits and fmt prints the sign, so
// every NaN reads as NaN, as the JSON encoder writes it
template <typename Number>
    requires std::integral<Number> || std::floating_point<Number>
void appendValueText(Number value, std::string& text) {
    if constexpr (std::floating_point<Number>) {
        if (std::isnan(value)) {
            text += "NaN";
            return;
        }
    }

    fmt::format_to(std::back_inserter(text), "{}", value);
}

// An entity an OPTIONAL MATCH did not match is an invalid ID, which reads as null
template <typename IDType>
    requires IsID<IDType>::value
void appendValueText(IDType id, std::string& text) {
    if (!id.isValid()) {
        text += "null";
        return;
    }

    appendValueText(id.getValue(), text);
}

template <int Tag, int Radix>
void appendValueText(const TemplateCommitHash<Tag, Radix>& hash, std::string& text) {
    hash.appendString(text);
}

void appendValueText(CustomBool value, std::string& text) {
    text += value ? "true" : "false";
}

void appendValueText(std::string_view value, std::string& text) {
    text += value;
}

void appendValueText(ValueType value, std::string& text) {
    text += ValueTypeName::value(value);
}

// A datetime or a duration renders in its ISO-8601 form rather than as its count, which is
// what every other reader of one shows
void appendValueText(DateTime value, std::string& text) {
    DateTime::format(text, value);
}

void appendValueText(Duration value, std::string& text) {
    Duration::format(text, value);
}

void appendValueText(PropertyNull value, std::string& text) {
    text += "null";
}

template <typename Sequence>
void appendElementsText(const Sequence& elements, std::string& text) {
    bool first = true;
    for (const auto& element : elements) {
        if (!first) {
            text += ", ";
        }

        first = false;
        appendValueText(element, text);
    }
}

// A path, read as its nodes and edges in order: a node as (id), an edge as [id]
void appendValueText(const GraphPath& path, std::string& text) {
    if (path.isNull()) {
        text += "null";
        return;
    }

    const std::span<const uint64_t> ids = path.getIDs();

    for (size_t position = 0; position < ids.size(); position++) {
        if (position > 0) {
            text += ", ";
        }

        if (GraphPath::isNodeAt(position)) {
            fmt::format_to(std::back_inserter(text), "({})", ids[position]);
        } else {
            fmt::format_to(std::back_inserter(text), "[{}]", ids[position]);
        }
    }
}

void appendValueText(types::Embedding::Primitive embedding, std::string& text) {
    text += '[';
    appendElementsText(embedding, text);
    text += ']';
}

void appendValueText(const ListView& list, std::string& text) {
    text += '[';
    appendElementsText(list, text);
    text += ']';
}

void appendValueText(const MapView& map, std::string& text) {
    text += '{';

    bool first = true;
    for (const MapEntryView entry : map) {
        if (!first) {
            text += ", ";
        }

        first = false;

        text += entry.getKey();
        text += ": ";
        appendValueText(entry.getValue(), text);
    }

    text += '}';
}

void appendValueText(CellView cell, std::string& text) {
    const auto appendTyped = [&text]<typename T>(CellView typed) {
        appendValueText(typed.getAs<T>(), text);
    };

    const CellTagDispatcher dispatcher {._tag = cell.getTag()};
    dispatcher.execute(appendTyped, cell);
}

// A cell is read as the value it holds, except that a list or an embedding filling a whole
// cell reads as its elements alone, without the brackets it is nested in
template <typename T>
void appendCellText(const T& value, std::string& text) {
    if constexpr (std::is_same_v<T, ListView> || std::is_same_v<T, types::Embedding::Primitive>) {
        appendElementsText(value, text);
    } else {
        appendValueText(value, text);
    }
}

template <typename T>
void appendCellText(const std::optional<T>& value, std::string& text) {
    if (!value) {
        text += "null";
        return;
    }

    appendCellText(*value, text);
}

struct CellTextWriter {
    std::string& _text;
    size_t _rowIndex {0};

    template <typename T>
    void operator()(const ColumnVector<T>* column) {
        appendCellText(column->getRaw()[_rowIndex], _text);
    }

    // A constant column holds the one value every row of the relation reads
    template <typename T>
    void operator()(const ColumnConst<T>* column) {
        appendCellText(column->getRaw(), _text);
    }

    void operator()(const ColumnMask* column) {
        _text += (*column)[_rowIndex] ? "true" : "false";
    }
};

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
    CellTextWriter writer {text, rowIndex};

    using Dispatcher = ColumnSingleDispatcher<OutputtedTypes::Allowed, CellTextWriter, OutputtedTypes::Excluded>;
    Dispatcher::dispatch(chunk, writer);

    return text;
}
