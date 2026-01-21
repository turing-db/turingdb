#pragma once

#include <nlohmann/json.hpp>

#include <spdlog/fmt/bundled/format.h>

#include "BatchVectorCreate.h"
#include "ImportResult.h"
#include "ImportException.h"

namespace vec {

using json = nlohmann::json;

/**
 * @brief JSON SAX parser for importing vectors
 *
 * This class parses JSON data for importing vectors, used by the `JsonImporter` class.
 *
 * ## Expected JSON Format
 *
 * ```json
 * [
 *     {
 *         "id": <id: integer>,
 *         "vector": [
 *             <float>, <float>, ...
 *         ]
 *     },
 *     ...
 * ]
 *
 * The state of the parser is stored in the `_state` variable. Here's how the state
 * changes while parsing the JSON data:
 *
 * ```json
 * // State::Start
 *
 * [ // State::ParsingData
 *
 *     { // State::ParsingEntry
 *         "id": // State::ParsingID
 *              <id: integer>, // State::ParsingEntry
 *         "vector":  // State::BeginParsingVector
 *         [ // State::ParsingVector
 *             <float>, <float>, ...
 *         ] // State::ParsingEntry
 *     }, // State::ParsingData
 *
 *     { // State::ParsingEntry
 *         "id": // State::ParsingID
 *              <id: integer>, // State::ParsingEntry
 *         "vector":  // State::BeginParsingVector
 *         [ // State::ParsingVector
 *             <float>, <float>, ...
 *         ] // State::ParsingEntry
 *     }, // State::ParsingData
 *     ...
 *
 * ] // State::End
 * ```
 */
class JsonSaxParser : public json::json_sax_t {
public:
    static constexpr uint64_t InvalidID = std::numeric_limits<uint64_t>::max();

    JsonSaxParser(BatchVectorCreate& batch)
        : _batch(batch),
        _dimension(batch.dimension())
    {
    }

    bool null() override {
        throwInvalidValue("null");
    }

    bool boolean(bool val) override {
        throwInvalidValue("boolean");
    }

    bool number_integer(number_integer_t val) override {
        throwInvalidValue("integer");
    }

    bool number_unsigned(number_unsigned_t val) override {
        if (_state != State::ParsingID) {
            throw ImportException {
                ImportErrorCode::JSONInvalidFormat,
                fmt::format("Unexpected unsigned integer `{}`, while parsing <{}>",
                            val,
                            StateDescription::value(_state))};
        }

        _currentID = val;
        _state = State::ParsingEntry;

        return true;
    }

    bool number_float(number_float_t val, const string_t&) override {
        if (_state != State::ParsingVector) {
            throw ImportException {
                ImportErrorCode::JSONInvalidFormat,
                fmt::format("Unexpected float `{}`, while parsing <{}>",
                            val,
                            StateDescription::value(_state))};
        }

        _currentVector.push_back(val);

        if (_currentVector.size() > _dimension) {
            throw ImportException {
                ImportErrorCode::InvalidVector,
                fmt::format("Vector should have {} dimensions", _dimension)};
        }

        return true;
    }

    bool string(string_t& val) override {
        throwInvalidValue("string");
    }

    bool start_object(std::size_t) override {
        switch (_state) {
            case State::Start: {
                throwError(ImportErrorCode::JSONInvalidFormat, "{", "`[`");
            } break;

            case State::ParsingData: {
                _state = State::ParsingEntry;
            } break;

            case State::ParsingEntry: {
                throwError(ImportErrorCode::JSONInvalidFormat, "{", "`\"id\"` or `\"vector\"`");
            } break;

            case State::ParsingID: {
                throwError(ImportErrorCode::JSONInvalidFormat, "{", "`unsigned integer`");
            } break;

            case State::BeginParsingVector: {
                throwError(ImportErrorCode::JSONInvalidFormat, "{", "`[...]`");
            } break;

            case State::ParsingVector: {
                throwError(ImportErrorCode::JSONInvalidFormat, "{", "`float` or `]`");
            } break;

            // Logic error:
            case State::End:
            case State::_SIZE: {
                throwCorruptedData("}");
            } break;
        }

        return true;
    }

    bool end_object() override {
        switch (_state) {
            case State::Start: {
                throw ImportException {ImportErrorCode::JSONInvalidFormat, std::string {"Input is empty"}};
            } break;

            case State::ParsingEntry: {
                if (_currentID == InvalidID) {
                    throw ImportException {ImportErrorCode::JSONMissingIDField};
                }

                if (_currentVector.size() != _dimension) {
                    throw ImportException {ImportErrorCode::InvalidVector,
                                           fmt::format("Vector has dimension '{}' instead of '{}'",
                                                       _currentVector.size(),
                                                       _dimension)};
                }

                _batch.addPoint(_currentID, _currentVector);
                _count++;

                _currentVector.clear();
                _currentID = InvalidID;

                _state = State::ParsingData;
            } break;

            // Logic error:
            case State::ParsingData:
            case State::ParsingID:
            case State::BeginParsingVector:
            case State::ParsingVector:
            case State::End:
            case State::_SIZE: {
                throwCorruptedData("}");
            } break;
        }

        return true;
    }

    bool start_array(std::size_t) override {
        switch (_state) {
            case State::Start: {
                _state = State::ParsingData;
            } break;

            case State::ParsingData: {
                throwError(ImportErrorCode::JSONInvalidFormat, "[", "`{`");
            } break;

            case State::ParsingEntry: {
                throwError(ImportErrorCode::JSONInvalidFormat, "[", "`\"id\"` or `\"vector\"`");
            } break;

            case State::ParsingID: {
                throwError(ImportErrorCode::JSONInvalidFormat, "[", "`unsigned integer`");
            } break;

            case State::BeginParsingVector: {
                _state = State::ParsingVector;
            } break;

            case State::ParsingVector: {
                throwError(ImportErrorCode::JSONInvalidFormat, "[", "`float` or `]`");
            } break;

            // Logic error:
            case State::End:
            case State::_SIZE: {
                throwCorruptedData("[");
            } break;
        }

        return true;
    }

    bool end_array() override {
        switch (_state) {
            case State::ParsingData: {
                _state = State::End;

                if (_count == 0) {
                    throw ImportException {ImportErrorCode::NoVectors};
                }
            } break;

            case State::ParsingVector: {
                _state = State::ParsingEntry;
            } break;

            // Logic error:
            case State::Start:
            case State::ParsingEntry:
            case State::ParsingID:
            case State::BeginParsingVector:
            case State::End:
            case State::_SIZE: {
                throwCorruptedData("]");
            } break;
        }

        return true;
    }

    bool key(string_t& val) override {
        if (_state == State::ParsingEntry) {
            if (val == "id") {
                _state = State::ParsingID;

                return true;
            } else if (val == "vector") {
                _state = State::BeginParsingVector;

                return true;
            }
        }

        throw ImportException {
            ImportErrorCode::JSONInvalidFormat,
            fmt::format("Unexpected key `{}` in `{}`",
                        val,
                        StateDescription::value(_state))};
    }

    bool binary(json::binary_t&) override {
        throwInvalidValue("binary");
    }

    bool parse_error(std::size_t,
                     const std::string&,
                     const json::exception& e) override {
        throw ImportException(ImportErrorCode::JSONParseError, e.what());
    }

    size_t parsedCount() const {
        return _count;
    }

    Dimension dimension() const {
        return _dimension;
    }

private:
    BatchVectorCreate& _batch;

    Dimension _dimension {0};
    size_t _count {0};
    uint64_t _currentID {InvalidID};
    std::vector<float> _currentVector;

    enum class State : uint8_t {
        Start = 0,
        ParsingData,
        ParsingEntry,
        ParsingID,
        BeginParsingVector,
        ParsingVector,
        End,

        _SIZE,
    } _state {State::Start};

    using StateDescription = EnumToString<State>::Create<
        EnumStringPair<State::Start, "start">,
        EnumStringPair<State::ParsingData, "vector list">,
        EnumStringPair<State::ParsingEntry, "vector entry">,
        EnumStringPair<State::ParsingID, "id field">,
        EnumStringPair<State::BeginParsingVector, "vector section">,
        EnumStringPair<State::ParsingVector, "vector section">,
        EnumStringPair<State::End, "end">>;

    [[noreturn]] void throwCorruptedData(std::string_view token) {
        throw ImportException {
            ImportErrorCode::JSONInvalidFormat,
            fmt::format("Unexpected `{}`, data corrupted", token)};
    }

    [[noreturn]] void throwError(ImportErrorCode code,
                                 std::string_view token,
                                 std::string_view expected = "") {
        if (expected.empty()) {
            throw ImportException {
                code,
                fmt::format("Unexpected `{}` while parsing {}",
                            token,
                            StateDescription::value(_state))};
        } else {

            throw ImportException {
                code,
                fmt::format("Unexpected `{}`, expected {} while parsing <{}>",
                            token,
                            expected,
                            StateDescription::value(_state))};
        }
    }

    [[noreturn]] void throwInvalidValue(std::string_view token) {
        throw ImportException {
            ImportErrorCode::JSONInvalidValue,
            fmt::format("Unexpected `{}`", token)};
    }
};

}
