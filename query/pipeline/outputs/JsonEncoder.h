#pragma once

#include <spdlog/fmt/bundled/format.h>

#include "QueryStatus.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "dataframe/Dataframe.h"
#include "OutputWriter.h"
#include "OutputValues.h"

namespace db {

template <Writer W>
struct ChunkJsonEncoder {
    W& _writer;
    bool _first = true;

    template <typename T>
    void operator()(const ColumnVector<T>* col) {
        _first = true;

        for (const T& value : *col) {
            if (!_first) {
                _writer.write(",");
            }

            encodeValue(value);
            _first = false;
        }
    }

    template <typename T>
    void operator()(const ColumnConst<T>* col) {
        encodeValue(col->getRaw());
    }

    template <Optional T>
    void encodeValue(const T& value) {
        if (!value.has_value()) {
            _writer.write("null");
        } else {
            encodeValue(*value);
        }
    }

    template <IDLike T>
    void encodeValue(const T& value) {
        _writer.write(std::to_string(value.getValue()));
    }

    template <Hash T>
    void encodeValue(const T& value) {
        _writer.write(std::to_string(value.get()));
    }

    template <std::integral T>
    void encodeValue(const T& value) {
        _writer.write(std::to_string(value));
    }

    template <std::floating_point T>
    void encodeValue(const T& value) {
        _writer.write(std::to_string(value));
    }

    void encodeValue(bool value) {
        _writer.write(value ? "true" : "false");
    }

    void encodeValue(ValueType value) {
        _writer.write(fmt::format("\"{}\"", ValueTypeName::value(value)));
    }

    template <std::convertible_to<std::string_view> T>
    void encodeValue(const T& value) {
        _writer.write(fmt::format("\"{}\"", value));
    }
};

template <Writer W>
class JsonEncoder {
public:
    JsonEncoder() = delete;

    JsonEncoder(const JsonEncoder&) = delete;
    JsonEncoder(JsonEncoder&&) = delete;
    JsonEncoder& operator=(const JsonEncoder&) = delete;
    JsonEncoder& operator=(JsonEncoder&&) = delete;

    JsonEncoder(W& writer)
        : _writer(writer)
    {
    }

    ~JsonEncoder() {
        finish();
    }

    void finish() noexcept {
        while (!_closingTokens.empty()) {
            end();
        }
    }

    void start() {
        obj();
    }

    void startData() {
        while (_closingTokens.size() > 1) {
            end();
        }

        key("data");
        arr();
    }

    void writeDataframeHeader(const Dataframe& df) {
        key("header");
        obj();

        key("column_names");
        arr();

        for (const NamedColumn* namedCol : df.cols()) {
            const std::string_view name = namedCol->getName();

            if (name.empty()) {
                const ColumnTag tag = namedCol->getTag();
                value(fmt::format("${}", tag.getValue()));
            } else {
                value(name);
            }
        }

        end(); // column_names

        key("column_types");
        arr();

        std::string columnName;
        ColumnTypeGenerator generator {columnName};

        for (const NamedColumn* namedCol : df.cols()) {
            const Column* col = namedCol->getColumn();

            using Types = OutputtedTypes;
            ColumnSingleDispatcher<Types::Allowed, ColumnTypeGenerator, Types::Excluded>::dispatch(col, generator);

            value(columnName);
        }

        end(); // column_types
        end(); // header

        startData();
    }

    void writeDataframe(const Dataframe& df) {
        arr();

        ChunkJsonEncoder<W> encoder {_writer};

        for (const NamedColumn* namedCol : df.cols()) {
            arr();
            const Column* col = namedCol->getColumn();

            using Types = OutputtedTypes;
            ColumnSingleDispatcher<Types::Allowed, ChunkJsonEncoder<W>, Types::Excluded>::dispatch(col, encoder);
            end();
        }

        end();
    }

    void obj() {
        _comma
            ? _writer.write(",{")
            : _writer.write("{");

        _closingTokens.push_back('}');
        _comma = false;
    }

    void arr() {
        _comma
            ? _writer.write(",[")
            : _writer.write("[");

        _closingTokens.push_back(']');
        _comma = false;
    }

    void end() noexcept {
        if (_closingTokens.empty()) {
            return;
        }

        _writer.write(_closingTokens.back());
        _closingTokens.pop_back();
        _comma = true;
    }

    void key(std::string_view k) {
        _comma
            ? _writer.write(fmt::format(",\"{}\":", k))
            : _writer.write(fmt::format("\"{}\":", k));

        _comma = false;
    }

    void encodeError(QueryStatus::Status status, std::string_view details) {
        while (_closingTokens.size() > 1) {
            end();
        }

        std::string_view errstr = QueryStatusDescription::value(status);
        std::string sanitizedDetails;
        sanitizeJsonString(details.empty()
                               ? "No error message available."
                               : details,
                           sanitizedDetails);

        key("error");
        value(errstr);

        key("error_details");
        value(sanitizedDetails);
    }

    void encodeTime(float milliseconds) {
        while (_closingTokens.size() > 1) {
            end();
        }

        key("time");
        _writer.write(std::to_string(milliseconds));
    }

    void value(std::string_view v) {
        _comma
            ? _writer.write(fmt::format(",\"{}\"", v))
            : _writer.write(fmt::format("\"{}\"", v));
        _comma = true;
    }

    static void sanitizeJsonString(std::string_view input, std::string& res) {
        res.reserve(input.size() * 1.2);

        for (char c : input) {
            if (c == '"') {
                res += "\\\"";
            } else if (c == '\\') {
                res += "\\\\";
            } else if (c == '\n') {
                res += "\\n";
            } else {
                res += c;
            }
        }
    }

private:
    W& _writer;
    std::string _closingTokens;
    bool _comma = false;
};

}
