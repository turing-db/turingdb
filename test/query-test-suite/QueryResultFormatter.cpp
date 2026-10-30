#include "QueryResultFormatter.h"

#include <optional>
#include <span>
#include <sstream>

#include <spdlog/fmt/bundled/format.h>

#include "BioAssert.h"
#include "GraphPath.h"
#include "PathView.h"
#include "ID.h"
#include "CellView.h"
#include "CellUtils.h"
#include "list/ListView.h"

#include "map/MapEntryView.h"
#include "map/MapView.h"
#include "QueryStatus.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitHash.h"

namespace db {

class CommitBuilder;
class Change;

}

namespace turing::test {

namespace {

[[maybe_unused]] std::string valueToString(const std::string& value) {
    return value;
}

[[maybe_unused]] std::string valueToString(const db::PathView value) {
    const std::span<const uint64_t> ids = value.getIDs();

    std::string result = "[";
    for (size_t position = 0; position < ids.size(); position++) {
        if (position > 0) {
            result += ", ";
        }

        if (db::GraphPath::isNodeAt(position)) {
            result += fmt::format("({})", ids[position]);
        } else {
            result += fmt::format("[{}]", ids[position]);
        }
    }

    result += "]";
    return result;
}

[[maybe_unused]] std::string valueToString(const db::GraphPath& value) {
    if (value.isNull()) {
        return "null";
    }

    return valueToString(db::PathView {value.getIDs()});
}

[[maybe_unused]] std::string valueToString(const std::string_view& value) {
    return std::string(value);
}

[[maybe_unused]] std::string valueToString(const db::ValueType& value) {
    return std::string(db::ValueTypeName::value(value));
}

template <db::IntegralType T, int tag>
[[maybe_unused]] std::string valueToString(const db::ID<T, tag> value) {
    return std::to_string(value.getValue());
}

template <int I, int Radix>
[[maybe_unused]] std::string valueToString(const db::TemplateCommitHash<I, Radix>& value) {
    return std::to_string(value.get());
}

[[maybe_unused]] std::string valueToString(const db::CustomBool& value) {
    return value ? "true" : "false";
}

[[maybe_unused]] std::string valueToString(const db::DateTime& value) {
    std::string result;
    db::DateTime::format(result, value);

    return result;
}

[[maybe_unused]] std::string valueToString(const db::Duration& value) {
    std::string result;
    db::Duration::format(result, value);

    return result;
}

[[maybe_unused]] std::string valueToString(const db::CommitBuilder* value) {
    return value ? "commit_builder_ptr" : "null";
}

[[maybe_unused]] std::string valueToString(const db::Change* value) {
    return value ? "change_ptr" : "null";
}

[[maybe_unused]] std::string valueToString(db::ListView view);

template <typename T>
[[maybe_unused]] std::string valueToString(const T& value) {
    return fmt::format("{}", value);
}

[[maybe_unused]] std::string valueToString(const db::PropertyNull) {
    return "null";
}

[[maybe_unused]] std::string valueToString(const std::span<const float>& value) {
    std::string result = "[";
    if (value.size() > 0) {
        result += std::to_string(value[0]);
        for (size_t i = 1; i < value.size(); ++i) {
            result += ",";
            result += std::to_string(value[i]);
        }
    }
    result += "]";
    return result;
}

[[maybe_unused]] std::string valueToString(const db::CellView element);
[[maybe_unused]] std::string valueToString(db::ListView view);
[[maybe_unused]] std::string valueToString(db::MapView view);

template <typename T>
[[maybe_unused]] std::string valueToString(const std::optional<T>& value) {
    if (!value.has_value()) {
        return "null";
    }

    return valueToString(*value);
}

[[maybe_unused]] std::string valueToString(const db::CellView element) {
    const auto writeTyped = []<typename T>(const db::CellView element) -> std::string {
        return valueToString(element.getAs<T>());
    };

    const db::CellTag tag = element.getTag();
    db::CellTagDispatcher writer {._tag = tag};

    return writer.execute(writeTyped, element);
}

[[maybe_unused]] std::string valueToString(const db::ListView view) {
    if (view.empty()) {
        return "[]";
    }

    std::string out = "[";
    size_t i = 0;

    for (const db::CellView element : view.elements()) {
        if (i++ > 0) {
            out += ", ";
        }
        out += valueToString(element);
    }

    out += "]";
    return out;
}

[[maybe_unused]] std::string entryToString(const db::MapEntryView entry) {
    std::string out {entry.getKey()};
    out += ": ";
    out += valueToString(entry.getValue());
    return out;
}

[[maybe_unused]] std::string valueToString(const db::MapView view) {
    if (view.empty()) {
        return "{}";
    }

    std::string out = "{";
    size_t i = 0;

    for (const db::MapEntryView entry : view.entries()) {
        if (i++ > 0) {
            out += ", ";
        }
        out += entryToString(entry);
    }

    out += "}";
    return out;
}

struct Stringify {
    std::string& _string;
    size_t _row {0};

    template <typename T>
    void operator()(const db::ColumnVector<T>* typed) {
        _string = valueToString(typed->at(_row));
    }

    template <typename T>
    void operator()(const db::ColumnConst<T>* typed) {
        _string = valueToString(typed->at(_row));
    }

    void operator()(const db::ColumnMask* typed) {
        _string = valueToString(typed->at(_row));
    }
};

std::string columnValueToString(const db::Column* column, size_t row) {
    std::string string;
    Stringify stringify(string, row);

    using Types = db::OutputtedTypes;
    using Dispatcher = db::ColumnSingleDispatcher<Types::Allowed, Stringify, Types::Excluded>;

    Dispatcher::dispatch(column, stringify);

    return string;
}

void escapeCsv(std::string& escaped, std::string_view value) {
    escaped.clear();

    bool needsQuotes = false;
    escaped.reserve(value.size());

    for (char ch : value) {
        if (ch == '"') {
            escaped.push_back('"');
            escaped.push_back('"');
            needsQuotes = true;
            continue;
        }

        if (ch == ',' || ch == '\n' || ch == '\r') {
            needsQuotes = true;
        }
        escaped.push_back(ch);
    }

    if (!needsQuotes) {
        escaped = value;
    } else {
        escaped = "\"" + escaped + "\"";
    }
}

std::string formatStatusError(const db::QueryStatus& status) {
    std::string statusName(db::QueryStatusDescription::value(status.getStatus()));

    for (char& ch : statusName) {
        if (ch == '_') {
            ch = ' ';
        }
    }

    if (!status.hasErrorMessage()) {
        return statusName;
    }

    return fmt::format("{}\n{}", statusName, status.getError());
}

} // namespace

void QueryResultFormatter::appendHeader(std::vector<std::string>& columnNames,
                                        const db::Dataframe* df) {
    bioassert(df != nullptr, "Dataframe is null");

    for (auto* col : df->cols()) {
        columnNames.emplace_back(col->getName());
    }
}

void QueryResultFormatter::appendRows(std::vector<std::vector<std::string>>& rows,
                                      std::vector<std::string>& values,
                                      const db::Dataframe* df) {
    bioassert(df != nullptr, "Dataframe is null");

    const size_t rowCount = df->getLogicalRowCount();
    for (size_t row = 0; row < rowCount; ++row) {
        values.clear();
        values.reserve(df->cols().size());

        for (auto* col : df->cols()) {
            values.push_back(columnValueToString(col->getColumn(), row));
        }

        rows.push_back(values);
    }
}

void QueryResultFormatter::appendChunkRows(std::vector<std::vector<std::string>>& rows,
                                           std::vector<std::string>& values,
                                           std::span<const db::Column* const> chunks,
                                           size_t offset,
                                           size_t rowCount) {
    for (size_t row = offset; row < offset + rowCount; ++row) {
        values.clear();
        values.reserve(chunks.size());

        for (const db::Column* col : chunks) {
            values.push_back(columnValueToString(col, row));
        }

        rows.push_back(values);
    }
}

std::string QueryResultFormatter::formatResultOutput(const db::QueryStatus& status,
                                                     const std::vector<std::string>& columnNames,
                                                     const std::vector<std::vector<std::string>>& rows) {
    if (!status.isOk()) {
        return formatStatusError(status);
    }

    std::stringstream resultOut;
    std::string escaped;

    for (size_t i = 0; i < columnNames.size(); ++i) {
        if (i > 0) {
            resultOut << ",";
        }

        escapeCsv(escaped, columnNames[i]);
        resultOut << escaped;
    }

    for (const auto& row : rows) {
        resultOut << "\n";
        for (size_t col = 0; col < row.size(); ++col) {
            if (col > 0) {
                resultOut << ",";
            }

            escapeCsv(escaped, row[col]);
            resultOut << escaped;
        }
    }

    return resultOut.str();
}

}
