#pragma once

#include <charconv>
#include <string_view>

#include "BasicResult.h"
#include "GMLError.h"

template <typename T>
concept GMLSaxInterface = requires(T t) {
    { t.onNodeProperty(std::string_view {}, std::string_view {}) } -> std::same_as<bool>;
    { t.onNodeProperty(std::string_view {}, uint64_t()) } -> std::same_as<bool>;
    { t.onNodeProperty(std::string_view {}, int64_t()) } -> std::same_as<bool>;
    { t.onNodeProperty(std::string_view {}, double()) } -> std::same_as<bool>;
    { t.onNodeID(uint64_t()) } -> std::same_as<bool>;
    { t.onEdgeProperty(std::string_view {}, std::string_view {}) } -> std::same_as<bool>;
    { t.onEdgeProperty(std::string_view {}, uint64_t()) } -> std::same_as<bool>;
    { t.onEdgeProperty(std::string_view {}, int64_t()) } -> std::same_as<bool>;
    { t.onEdgeProperty(std::string_view {}, double()) } -> std::same_as<bool>;
    { t.onEdgeSource(uint64_t()) } -> std::same_as<bool>;
    { t.onEdgeTarget(uint64_t()) } -> std::same_as<bool>;
    { t.onNodeBegin() } -> std::same_as<bool>;
    { t.onNodeEnd() } -> std::same_as<bool>;
    { t.onEdgeBegin() } -> std::same_as<bool>;
    { t.onEdgeEnd() } -> std::same_as<bool>;
};

#define CHECK_UNEXPECTED_EOF(expected)                                                        \
    if (_rem.empty()) {                                                                       \
        res = db::GMLError::result(_line, "Unexpected end of file. Expected: '{}'", expected); \
        return res;                                                                           \
    }

#define UNEXPECTED_TOKEN(unexpected, expected)                                                       \
    res = db::GMLError::result(_line, "Unexpected token '{}'. Expected: '{}'", unexpected, expected); \
    return res;

template <GMLSaxInterface Interface>
class GMLParser {
public:
    explicit GMLParser(Interface& interface)
        : _interface(interface)
    {
    }

    ~GMLParser() = default;

    GMLParser(const GMLParser&) = delete;
    GMLParser(GMLParser&&) = delete;
    GMLParser& operator=(const GMLParser&) = delete;
    GMLParser& operator=(GMLParser&&) = delete;

    using StringResult = BasicResult<std::string_view, db::GMLError>;
    using UintResult = BasicResult<uint64_t, db::GMLError>;

    [[nodiscard]] db::GMLParseResult parse(std::string_view data) {
        _rem = data;
        _state = State::GraphRoot;
        _line = 1;

        db::GMLParseResult res;
        GMLParser::StringResult strRes;
        GMLParser::UintResult uintRes;

        skipBlanks();

        while (true) {
            switch (_state) {
                case State::GraphRoot: {
                    CHECK_UNEXPECTED_EOF("graph");

                    const auto strRes = this->getNextWord(Tokens::GRAPH);
                    if (!strRes) {
                        return strRes.get_unexpected();
                    }

                    if (res = this->checkToken(strRes.value(), Tokens::GRAPH); !res) {
                        return res;
                    }

                    if (res = this->checkToken(Tokens::OB); !res) {
                        return res;
                    }

                    _state = State::NextEntity;
                } break;
                case State::NextEntity: {
                    const char c = *_rem.data();
                    CHECK_UNEXPECTED_EOF("]");

                    if (c == ']') {
                        next();
                        skipBlanks();
                        if (!_rem.empty()) {
                            // We should have reached the end of the file by now
                            const auto strRes = this->getNextWord("Edge/Node");
                            if (this->checkToken(strRes.value(), Tokens::GRAPH)) {
                                res = db::GMLError::result(_line, "Multigraph parsing is not supported yet");
                                return res;
                            }
                            UNEXPECTED_TOKEN(*_rem.data(), "end of file");
                        }
                        return {}; // Valid end of file
                    }

                    if (!(std::isalpha(c))) {
                        UNEXPECTED_TOKEN(c, "alphabet");
                    }

                    const auto strRes = this->getNextWord("Edge/Node");
                    if (!strRes) {
                        return strRes.get_unexpected();
                    }

                    if (this->checkToken(strRes.value(), Tokens::NODE)) {
                        if (res = this->checkToken(Tokens::OB); !res) {
                            return res;
                        }
                        _nodeID = std::numeric_limits<uint64_t>::max();
                        if (!_interface.onNodeBegin()) {
                            res = db::GMLError::result(_line, "OnNodeBegin event failed");
                            return res;
                        }

                        _state = State::NodePropertyKey;
                        break;
                    }

                    if (this->checkToken(strRes.value(), Tokens::EDGE)) {
                        if (res = this->checkToken(Tokens::OB); !res) {
                            return res;
                        }

                        _sourceID = std::numeric_limits<uint64_t>::max();
                        _targetID = std::numeric_limits<uint64_t>::max();

                        if (!_interface.onEdgeBegin()) {
                            res = db::GMLError::result(_line, "OnEdgeBegin event failed");
                            return res;
                        }

                        _state = State::EdgePropertyKey;
                        break;
                    }

                    _state = State::GraphAttributeKey;

                } break;
                case State::NodePropertyKey: {
                    CHECK_UNEXPECTED_EOF("]");

                    const char c = *_rem.data();

                    if (c == ']') {
                        next();
                        skipBlanks();

                        if (_propNesting != 0) {
                            decreasePropNesting();
                            break;
                        }

                        _state = State::NextEntity;

                        if (_nodeID == std::numeric_limits<uint64_t>::max()) {
                            UNEXPECTED_TOKEN(']', "node ID");
                        }

                        if (!_interface.onNodeEnd()) {
                            res = db::GMLError::result(_line, "OnNodeEnd event failed");
                            return res;
                        }

                        break; // Node complete
                    }

                    if (checkCurrentWord(Tokens::ID)) {
                        next(Tokens::ID.size());
                        skipBlanks();
                        _state = State::NodeID;
                        break;
                    }

                    strRes = this->getNextWord("node property value");
                    if (!strRes) {
                        return strRes.get_unexpected();
                    }

                    setCurrentKeyAndSkip(strRes.value());

                    _state = State::NodePropertyValue;
                } break;
                case State::EdgePropertyKey: {
                    CHECK_UNEXPECTED_EOF("]");

                    const char c = *_rem.data();
                    if (c == ']') {
                        next();
                        skipBlanks();

                        if (_propNesting != 0) {
                            decreasePropNesting();
                            break;
                        }

                        _state = State::NextEntity;

                        if (_sourceID == std::numeric_limits<uint64_t>::max()) {
                            UNEXPECTED_TOKEN(']', "edge source ID");
                        }

                        if (_targetID == std::numeric_limits<uint64_t>::max()) {
                            UNEXPECTED_TOKEN(']', "edge target ID");
                        }

                        if (!_interface.onEdgeEnd()) {
                            res = db::GMLError::result(_line, "OnEdgeEnd event failed");
                            return res;
                        }

                        break; // Edge complete
                    }

                    if (checkCurrentWord(Tokens::SOURCE)) {
                        next(Tokens::SOURCE.size());
                        skipBlanks();
                        _state = State::EdgeSource;
                        break;
                    }

                    if (checkCurrentWord(Tokens::TARGET)) {
                        next(Tokens::TARGET.size());
                        skipBlanks();
                        _state = State::EdgeTarget;
                        break;
                    }

                    strRes = this->getNextWord("edge property value");
                    if (!strRes) {
                        return strRes.get_unexpected();
                    }

                    setCurrentKeyAndSkip(strRes.value());

                    _state = State::EdgePropertyValue;
                } break;
                case State::NodeID: {
                    CHECK_UNEXPECTED_EOF("node ID (unsigned integer)");

                    uintRes = this->parseID();
                    if (!uintRes) {
                        return uintRes.get_unexpected();
                    }

                    _nodeID = uintRes.value();
                    if (!_interface.onNodeID(_nodeID)) {
                        res = db::GMLError::result(_line, "OnNodeID event failed: {}", _nodeID);
                        return res;
                    }

                    _state = State::NodePropertyKey;
                } break;
                case State::NodePropertyValue: {
                    CHECK_UNEXPECTED_EOF("property value");

                    if (*_rem.data() == '"') {
                        next();
                        CHECK_UNEXPECTED_EOF('"');

                        strRes = this->parseQuotedString();
                        if (!strRes) {
                            return strRes.get_unexpected();
                        }

                        if (!_interface.onNodeProperty(_currentKey, strRes.value())) {
                            res = db::GMLError::result(_line,
                                                   "OnNodeProperty event failed: {{{}: {}}}",
                                                   _currentKey,
                                                   strRes.value());
                            return res;
                        }

                        _state = State::NodePropertyKey;
                        break;
                    }

                    if (*_rem.data() == '[') {
                        increasePropNesting();
                        _state = State::NodePropertyKey;
                        break;
                    }

                    if (*_rem.data() == ']') {
                        UNEXPECTED_TOKEN(']', "property value");
                    }

                    strRes = this->getNextWord("property value");
                    if (!strRes) {
                        return strRes.get_unexpected();
                    }

                    // TODO Implement ints, booleans and floats
                    if (!_interface.onNodeProperty(_currentKey, strRes.value())) {
                        res = db::GMLError::result(_line,
                                               "OnNodeProperty event failed: {{{}: {}}}",
                                               _currentKey,
                                               strRes.value());
                        return res;
                    }

                    next(strRes.value().size());
                    skipBlanks();

                    _state = State::NodePropertyKey;
                } break;
                case State::EdgePropertyValue: {
                    CHECK_UNEXPECTED_EOF("property value");

                    if (*_rem.data() == '"') {
                        next();
                        CHECK_UNEXPECTED_EOF('"');

                        strRes = this->parseQuotedString();
                        if (!strRes) {
                            return strRes.get_unexpected();
                        }

                        if (!_interface.onEdgeProperty(_currentKey, strRes.value())) {
                            res = db::GMLError::result(_line,
                                                   "OnEdgeProperty event failed: {{{}: {}}}",
                                                   _currentKey,
                                                   strRes.value());
                            return res;
                        }
                        _state = State::EdgePropertyKey;
                        break;
                    }

                    if (*_rem.data() == '[') {
                        increasePropNesting();
                        _state = State::EdgePropertyKey;
                        break;
                    }

                    if (*_rem.data() == ']') {
                        UNEXPECTED_TOKEN(']', "property value");
                    }

                    strRes = this->getNextWord("property value");
                    if (!strRes) {
                        return strRes.get_unexpected();
                    }

                    // TODO Implement ints, booleans and floats
                    if (!_interface.onEdgeProperty(_currentKey, strRes.value())) {
                        res = db::GMLError::result(_line,
                                               "OnEdgeProperty event failed: {{{}: {}}}",
                                               _currentKey,
                                               strRes.value());
                        return res;
                    }

                    next(strRes.value().size());
                    skipBlanks();

                    _state = State::EdgePropertyKey;
                } break;
                case State::EdgeSource: {
                    CHECK_UNEXPECTED_EOF("source ID (unsigned integer)");

                    uintRes = this->parseID();
                    if (!uintRes) {
                        return uintRes.get_unexpected();
                    }

                    _sourceID = uintRes.value();
                    if (!_interface.onEdgeSource(_sourceID)) {
                        res = db::GMLError::result(_line, "OnEdgeSource event failed: {}", _sourceID);
                        return res;
                    }

                    _state = State::EdgePropertyKey;
                } break;
                case State::EdgeTarget: {
                    CHECK_UNEXPECTED_EOF("target ID (unsigned integer)");

                    const auto uintRes = this->parseID();
                    if (!uintRes) {
                        return uintRes.get_unexpected();
                    }

                    _targetID = uintRes.value();
                    if (!_interface.onEdgeTarget(_targetID)) {
                        res = db::GMLError::result(_line, "OnEdgeTarget event failed: {}", _targetID);
                        return res;
                    }

                    _state = State::EdgePropertyKey;
                } break;
                case State::GraphAttributeKey: {
                    strRes = this->getNextWord("Graph Attribute Key");
                    if (!strRes) {
                        return strRes.get_unexpected();
                    }
                    next(strRes.value().size());
                    skipBlanks();

                    _state = State::GraphAttributeValue;

                } break;
                case State::GraphAttributeValue: {
                    if (*_rem.data() == '"') {
                        next();
                        CHECK_UNEXPECTED_EOF('"');

                        strRes = this->parseQuotedString();
                        if (!strRes) {
                            return strRes.get_unexpected();
                        }

                        _state = State::NextEntity;
                        break;
                    }

                    strRes = this->getNextWord("Graph Attribute Value");
                    if (!strRes) {
                        return strRes.get_unexpected();
                    }
                    next(strRes.value().size());
                    skipBlanks();

                    _state = State::NextEntity;

                } break;
            }
        }
    }

    void next(size_t inc = 1) {
        _rem = _rem.substr(inc, _rem.size() - inc);
    }

private:
    Interface& _interface;

    enum class State {
        GraphRoot = 0,
        NextEntity,
        NodePropertyKey,
        NodePropertyValue,
        NodeID,
        EdgePropertyKey,
        EdgePropertyValue,
        EdgeSource,
        EdgeTarget,
        GraphAttributeKey,
        GraphAttributeValue,
    };

    State _state {};
    size_t _line {0};
    std::string_view _rem;
    std::string _currentKey;
    uint64_t _nodeID = std::numeric_limits<uint64_t>::max();
    uint64_t _sourceID = std::numeric_limits<uint64_t>::max();
    uint64_t _targetID = std::numeric_limits<uint64_t>::max();
    size_t _propNesting {0};
    std::vector<size_t> _propPrefixSizes = {0};

    class Tokens {
    public:
        static constexpr std::string_view GRAPH = "graph";
        static constexpr std::string_view NODE = "node";
        static constexpr std::string_view EDGE = "edge";
        static constexpr std::string_view ID = "id";
        static constexpr std::string_view SOURCE = "source";
        static constexpr std::string_view TARGET = "target";
        static constexpr auto OB = '[';
        static constexpr auto CB = ']';
    };

    void skipBlanks() {
        while (!_rem.empty() && ::isspace(*_rem.data())) {
            if (*_rem.data() == '\n') {
                _line++;
            }
            next(1);
        }
    }

    db::GMLParseResult checkToken(std::string_view parsedToken, std::string_view expectedToken) {
        if (parsedToken != expectedToken) {
            return db::GMLError::result(_line,
                                    "Unexpected token '{}'. Expected: '{}'",
                                    parsedToken,
                                    expectedToken);
        }

        next(expectedToken.size());
        skipBlanks();
        return {};
    }

    db::GMLParseResult checkToken(char token) {
        if (_rem.empty()) {
            return db::GMLError::result(_line,
                                    "Unexpected end of file. Expected: '{}'",
                                    token);
        }

        if (*_rem.data() != token) {
            return db::GMLError::result(_line,
                                    "Unexpected token '{}'. Expected: '{}'",
                                    *_rem.data(),
                                    token);
        }

        next();
        skipBlanks();
        return {};
    }

    StringResult getNextWord(std::string_view nextToken) {
        const char* it = _rem.begin();
        while (*it != '\n' && *it != ' ' && *it != ']'
               && *it != '[' && *it != '\t' && *it != '\r'
               && *it != '\v' && *it != '\f') {
            it++;

            if (it == _rem.end()) {
                return db::GMLError::result(_line,
                                        "Unexpected end of file. Expected: '{}'",
                                        nextToken);
            }
        }

        const size_t charCount = std::distance(_rem.begin(), it);
        return _rem.substr(0, charCount);
    }

    StringResult parseQuotedString() {
        const char* it = _rem.begin();
        const char* end = _rem.end();
        for (;;) {
            const char c = *it;

            if (c == '\\') {
                it++;

                if (it == end) {
                    return db::GMLError::result(_line,
                                            "Unexpected end of file, Expected: '\"'",
                                            *_rem.data());
                }

                it++;

                if (it == end) {
                    return db::GMLError::result(_line,
                                            "Unexpected end of file, Expected: '\"'",
                                            *_rem.data());
                }
                continue;
            }

            if (*it == '"') {
                const size_t charCount = std::distance(_rem.begin(), it);
                const std::string_view res = _rem.substr(0, charCount);
                next(charCount + 1);
                skipBlanks();
                return res;
            }

            it++;

            if (it == _rem.end()) {
                return db::GMLError::result(_line, "Unexpected end of file. Expected: 'property value'");
            }
        }
    }

    UintResult parseID() {
        // Gathering
        auto wordRes = this->getNextWord("ID (unsigned integer)");

        if (!wordRes) {
            return wordRes.get_unexpected();
        }

        std::string_view word = wordRes.value();
        uint64_t id {0};
        const auto res = std::from_chars(word.begin(), word.end(), id);

        if (res.ec == std::errc::result_out_of_range) {
            return db::GMLError::result(_line,
                                    "Unexpected token '{}' (integer overflow). Expected: 'ID (unsigned integer)'",
                                    _rem.substr(0, word.size()));
        } else if (res.ec == std::errc::invalid_argument
                   || (size_t)std::distance(_rem.begin(), res.ptr) != word.size()) {
            return db::GMLError::result(_line,
                                    "Unexpected token '{}'. Expected: 'ID (unsigned integer)'",
                                    _rem.substr(0, word.size()));
        }

        next(word.size());
        skipBlanks();
        return id;
    }

    bool checkCurrentWord(std::string_view token) {
        if (_rem.size() < token.size() + 1) {
            return false;
        }

        const auto substr = _rem.substr(0, token.size());
        return substr == token && ::isspace(_rem[token.size()]);
    };

    void increasePropNesting() {
        next();
        skipBlanks();

        _propNesting++;
        _currentKey += "_";
        _propPrefixSizes.push_back(_currentKey.size());
    }

    void decreasePropNesting() {
        _propNesting--;
        _propPrefixSizes.pop_back();
        _currentKey.resize(_propPrefixSizes.back());
        _state = State::NodePropertyKey;
    }

    void setCurrentKeyAndSkip(std::string_view key) {
        _currentKey.resize(_propPrefixSizes.back());
        _currentKey += key;
        next(key.size());
        skipBlanks();
    }
};

