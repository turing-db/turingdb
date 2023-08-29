#include "GMLLexer.h"

#include <ctype.h>

namespace {

bool isBlank(char c) {
    return (c == ' ') || (c == '\t') || (c == '\r') || (c == '\n');
}

}

GMLLexer::GMLLexer(const char* buffer, size_t size)
    : _bufferStart(buffer),
    _current(buffer),
    _end(buffer+size)
{
}

GMLLexer::~GMLLexer() {
}

void GMLLexer::skipWhitespaces() {
    while (_current < _end && isBlank(*_current)) {
        if (*_current == '\n') {
            _lineCount++;
        }
        _current++;
    }
}

void GMLLexer::nextToken() {
    skipWhitespaces();

    if (_current >= _end) {
        _token.setToken(GMLToken::TK_END, _current, 0);
        return;
    }

    const char c = *_current;
    if (isalpha(c) && c != '-') {
        lexString();
    } else if (isdigit(c) || c == '-') {
        lexInt();
    } else if (c == '[') {
        _token.setToken(GMLToken::TK_OSBRACK, _current, 1);
        _current++;
    } else if (c == ']') {
        _token.setToken(GMLToken::TK_CSBRACK, _current, 1);
        _current++;
    } else if (c == '"') {
        lexString();
    } else {
        _token.setToken(GMLToken::TK_ERROR, _current, 1);
    }
}

void GMLLexer::lexInt() {
    const char* start = _current;

    while (_current < _end && (isdigit(*_current) || *_current == '-')) {
        _current++;
    }

    GMLToken::TokenType tokenType = GMLToken::TK_INT;
    if (_current < _end && *_current == '.') {
        _current++;

        if ((_current >= _end || !isdigit(*_current)) && *_current != 'e') {
            _token.setToken(GMLToken::TK_ERROR, start, _current-start-1);
            return;
        }

        tokenType = GMLToken::TK_DOUBLE;
        while (_current < _end && (isdigit(*_current) || *_current == 'e' || *_current == '-')) {
            _current++;
        }
    }

    const size_t tokSize = _current-start;
    _token.setToken(tokenType, start, tokSize);
}

void GMLLexer::lexString() {
    const char* start = _current;

    if (*start == '"') {
        _current++;
        while (_current < _end && *_current != '"') {
            _current++;
        }

        if (*_current != '"') {
            _token.setToken(GMLToken::TK_ERROR, start+1, _current-start);
        } else {
            _token.setToken(GMLToken::TK_STRING, start+1, _current-start-1);
            _current++;
        }
    } else {
        while (_current < _end && !isBlank(*_current)) {
            _current++;
        }

        const size_t tokSize = _current-start;
        _token.setToken(GMLToken::TK_STRING, start, tokSize);
    }
}
