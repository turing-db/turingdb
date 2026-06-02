#pragma once

#include <ranges>
#include <spdlog/fmt/bundled/format.h>
#include <range/v3/view/drop.hpp>

#include "QueryStatus.h"

#include "list/ListElementView.h"
#include "list/ListUtils.h"

#include "map/MapEntryView.h"
#include "map/MapUtils.h"
#include "map/MapView.h"

#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "dataframe/Dataframe.h"

#include "OutputWriter.h"
#include "OutputValues.h"

#include "ControlCharacters.h"

namespace rg = ranges;
namespace rv = ranges::views;

namespace db {

template <Writer WriterT>
class ChunkJsonEncoder {
public:
    ChunkJsonEncoder(WriterT& writer, size_t rowcnt)
        : _writer(writer),
        _logicalRowCount(rowcnt)
    {
    }

    ~ChunkJsonEncoder() = default;

    template <typename T>
    void operator()(const ColumnVector<T>* col) {
        if (_logicalRowCount == 0) {
            return;
        }

        const T& firstValue = col->operator[](0);
        encodeValue(firstValue);

        for (size_t row = 1; row < _logicalRowCount; row++) {
            _writer.write(",");

            const T& value = col->operator[](row);

            encodeValue(value);
        }
    }

    template <typename T>
    void operator()(const ColumnConst<T>* col) {
        if (_logicalRowCount == 0) {
            return;
        }

        const T& firstValue = col->operator[](0);
        encodeValue(firstValue);

        for (size_t row = 1; row < _logicalRowCount; row++) {
            _writer.write(",");

            const T& value = col->operator[](row);

            encodeValue(value);
        }
    }

private:
    WriterT& _writer;
    const size_t _logicalRowCount {0};
    std::string _sanitized;

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

    void encodeValue(const Path& value) {
        _writer.write("[");
        if(!value.empty()) {
            auto it = value.rbegin();
            _writer.write(it->getValue());
            ++it;
            while(it != value.rend()) {
                _writer.write(",");
                _writer.write(it->getValue());
                ++it;
            }
        }
        _writer.write("]");
    }

    void encodeValue(ValueType value) {
        ControlCharactersEscaper::escapeAndSurroundByQuotes(ValueTypeName::value(value), _sanitized);
        _writer.write(_sanitized);
    }

    void encodeValue(PropertyNull) {
        _writer.write("null");
    }

    void encodeValue(types::Embedding::Primitive value) {
        _writer.write("[");
        if (value.size() > 0) {
            _writer.write(std::to_string(value[0]));
            for (size_t i = 1; i < value.size(); i++) {
                _writer.write(",");
                _writer.write(std::to_string(value[i]));
            }
        }
        _writer.write("]");
    }

    void encodeValue(const std::optional<types::Embedding::Primitive>& value) {
        if (!value.has_value()) {
            _writer.write("null");
        } else {
            encodeValue(*value);
        }
    }

    template <std::convertible_to<std::string_view> T>
    void encodeValue(const T& value) {
        ControlCharactersEscaper::escapeAndSurroundByQuotes(value, _sanitized);
        _writer.write(_sanitized);
    }

    void encodeValue(const EntityList& value) {
        if (value.empty()) {
            _writer.write("[]");
            return;
        }

        const auto& entries = value.getEntries();
        const auto& firstValue = entries.front();

        _writer.write("[{\"type\":\"");
        _writer.write(firstValue._type == EntityType::Node ? "node" : "edge");
        _writer.write("\",\"id\":");
        _writer.write(std::to_string(firstValue._id.getValue()));
        _writer.write('}');

        for (const auto& [type, id] : entries | std::views::drop(1)) {
            _writer.write(',');
            _writer.write('{');
            _writer.write("\"type\":\"");
            _writer.write(type == EntityType::Node ? "node" : "edge");
            _writer.write("\",\"id\":");
            _writer.write(std::to_string(id.getValue()));
            _writer.write('}');
        }
        _writer.write("]");
    }

    void encodeValue(const ListElementView ele) {
        const auto writeTyped = [this]<typename T>(const ListElementView ele) {
            const T typed = ele.getAs<T>();
            this->encodeValue(typed);
        };

        const ListBufferTypeTag tag = ele.getTag();
        ListTagDispatcher writer {._tag = tag};
        writer.execute(writeTyped, ele);
    }

    void encodeValue(const ListView lv) {
        if (lv.empty()) {
            _writer.write("[]");
            return;
        }

        _writer.write('[');

        const ListElementView fst = lv.front();
        encodeValue(fst);

        for (const ListElementView ele : lv.elements() | rv::drop(1)) {
            _writer.write(", ");
            encodeValue(ele);
        }

        _writer.write(']');
    }

    void encodeValue(const MapEntryView entry) {
        encodeValue(entry.getKey());
        _writer.write(": ");

        const auto writeTyped = [this]<typename T>(const MapEntryView entry) {
            const T typed = entry.getValueAs<T>();
            this->encodeValue(typed);
        };

        const MapBufferTypeTag tag = entry.getValueTag();
        MapTagDispatcher writer {._tag = tag};
        writer.execute(writeTyped, entry);
    }

    void encodeValue(const MapView mv) {
        if (mv.empty()) {
            _writer.write("{}");
            return;
        }

        _writer.write('{');

        const MapEntryView fst = mv.front();
        encodeValue(fst);

        for (const MapEntryView entry : mv.entries() | rv::drop(1)) {
            _writer.write(", ");
            encodeValue(entry);
        }

        _writer.write('}');
    }
};

template <Writer WriterT>
class JsonEncoder {
public:
    JsonEncoder() = delete;

    JsonEncoder(const JsonEncoder&) = delete;
    JsonEncoder(JsonEncoder&&) = delete;
    JsonEncoder& operator=(const JsonEncoder&) = delete;
    JsonEncoder& operator=(JsonEncoder&&) = delete;

    JsonEncoder(WriterT& writer)
        : _writer(writer)
    {
    }

    ~JsonEncoder() {
    }

    void finish() {
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

        std::string columnType;
        ColumnTypeGenerator generator(columnType);

        using Types = OutputtedTypes;
        using ColTypeGen = ColumnSingleDispatcher<Types::Allowed, ColumnTypeGenerator, Types::Excluded>;

        for (const NamedColumn* namedCol : df.cols()) {
            const Column* col = namedCol->getColumn();

            ColTypeGen::dispatch(col, generator);

            value(columnType);
        }

        end(); // column_types
        end(); // header

        startData();
    }

    void writeDataframe(const Dataframe& df) {
        arr();

        const size_t logicalRowCount = df.getLogicalRowCount();

        using JsonWriter = ChunkJsonEncoder<WriterT>;
        using Types = OutputtedTypes;
        using Encoder = ColumnSingleDispatcher<Types::Allowed, JsonWriter, Types::Excluded>;

        JsonWriter encoder(_writer, logicalRowCount);
        for (const NamedColumn* namedCol : df.cols()) {
            arr();

            const Column* col = namedCol->getColumn();

            Encoder::dispatch(col, encoder);

            end();
        }

        end();
    }

    void obj() {
        if (_comma) {
            _writer.write(",{");
        } else {
            _writer.write("{");
        }

        _closingTokens.push_back('}');
        _comma = false;
    }

    void arr() {
        if (_comma) {
            _writer.write(",[");
        } else {
            _writer.write("[");
        }

        _closingTokens.push_back(']');
        _comma = false;
    }

    void end() {
        if (_closingTokens.empty()) {
            return;
        }

        _writer.write(_closingTokens.back());
        _closingTokens.pop_back();
        _comma = true;
    }

    void key(std::string_view k) {
        ControlCharactersEscaper::escape(k, _sanitized);
        if (_comma) {
            _writer.write(fmt::format(",\"{}\":", _sanitized));
        } else {
            _writer.write(fmt::format("\"{}\":", _sanitized));
        }

        _comma = false;
    }

    void encodeError(QueryStatus::Status status, std::string_view details) {
        while (_closingTokens.size() > 1) {
            end();
        }

        // Escape error message
        const std::string_view errstr = QueryStatusDescription::value(status);

        key("error");
        value(errstr);

        key("error_details");
        value(details.empty() ? "No error message available." : details);
    }

    void encodeTime(float milliseconds) {
        while (_closingTokens.size() > 1) {
            end();
        }

        key("time");
        _writer.write(std::to_string(milliseconds));
    }

    void value(std::string_view v) {
        ControlCharactersEscaper::escapeAndSurroundByQuotes(v, _sanitized);
        if (_comma) {
            _writer.write(',');
            _writer.write(_sanitized);
        } else {
            _writer.write(_sanitized);
        }

        _comma = true;
    }

private:
    WriterT& _writer;
    std::string _closingTokens;
    bool _comma {false};
    std::string _sanitized;
};

}
