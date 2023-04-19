#pragma once

#include <string_view>

class GMLToken {
public:
    enum TokenType {
        TK_ERROR = 0,
        TK_END,
        TK_STRING,
        TK_OSBRACK,
        TK_CSBRACK,
        TK_INT,
        TK_DOUBLE,
        TK_QUOTE
    };

    GMLToken() = default;

    TokenType getType() const { return _type; }

    std::string_view getData() const { return _data; }

    bool isEnd() const { return _type == TK_END; }
    bool isError() const { return _type == TK_ERROR; }
    bool isCSBRACK() const { return _type == TK_CSBRACK; }

    void setToken(TokenType type, const char* start, size_t size) {
        setToken(type, std::string_view(start, size));
    }

    void setToken(TokenType type, std::string_view data) {
        _type = type;
        _data = data;
    }

private:
    TokenType _type {TK_ERROR};
    std::string_view _data;
};

class GMLLexer {
public:
    GMLLexer(const char* buffer, size_t size);
    ~GMLLexer();

    size_t getLine() const { return _lineCount; }
    size_t getOffset() const { return _current-_bufferStart; }

    void nextToken();

    const GMLToken& getToken() const { return _token; }

private:
    const char* const _bufferStart {nullptr};
    const char* _current {nullptr};
    const char* const _end {nullptr};
    GMLToken _token;
    size_t _lineCount {1};

    void skipWhitespaces();
    void lexInt();
    void lexString();
};
