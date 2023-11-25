#include "HTTPParser.h"

using namespace net;

static constexpr size_t MIN_METHOD_SIZE = 3;

namespace {

bool isBlank(char c) {
    return (c == ' ') || (c == '\n') || (c == '\r') || (c == '\t');
}

bool isURIValid(char c) {
    return (c == '/') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
        || (c >= '0' && c <= '9') || (c == '_' || c == '=' || c == '&' || c == ';');
}

}

HTTPParser::HTTPParser(Buffer* inputBuffer)
    : _reader(inputBuffer->getReader()),
    _currentPtr(_reader.getData())
{
}

bool HTTPParser::analyze() {
    if (!parseRequestLine()) {
        return false;
    }

    if (!jumpToPayload()) {
        return false;
    }

    _payload = StringSpan(_currentPtr, getSize());

    return true;
}

bool HTTPParser::parseRequestLine() {
    if (getSize() == 0) {
        return false;
    }

    if (!parseMethod()) {
        return false;
    }

    if (!parseURI()) {
        return false;
    }

    return true;
}

bool HTTPParser::parseMethod() {
    const char c = *_currentPtr;
    if (c == 'P') {
        return parsePOST();
    } else if (c == 'G') {
        return parseGET();
    }

    return false;
}

bool HTTPParser::parsePOST() {
    if (getSize() < 4) {
        return false;
    }

    const bool isPOST = _currentPtr[1] == 'O'
        && _currentPtr[2] == 'S'
        && _currentPtr[3] == 'T';
    if (!isPOST) {
        return false;
    }

    _method = HTTPMethod::POST;
    _currentPtr += 4;

    return true;
}

bool HTTPParser::parseGET() {
    if (getSize() < 4) {
        return false;
    }

    const bool isGET = _currentPtr[1] == 'E'
        && _currentPtr[2] == 'T'
        && _currentPtr[3] == ' ';
    if (!isGET) {
        return false;
    }

    _method = HTTPMethod::GET;

    _currentPtr += 4;
    return true;
}

bool HTTPParser::parseURI() {
    if (getSize() == 0) {
        return false;
    }

    _uri.clear();
    const char* endPtr = getEndPtr();
    for (; _currentPtr < endPtr; _currentPtr++) {
        if (*_currentPtr != ' ') {
            break;
        }
    }

    for (; _currentPtr < endPtr; _currentPtr++) {
        const char c = *_currentPtr;
        if (isBlank(c)) {
            break;
        }
        if (!isURIValid(c)) {
            return false;
        }

        _uri.push_back(c);
    }

    return !_uri.empty();
}

bool HTTPParser::jumpToPayload() {
    const char* endPtr = getEndPtr();

    while (endPtr-_currentPtr >= 4) {
        const bool isEmptyLine = (_currentPtr[0] == '\r'
            && _currentPtr[1] == '\n'
            && _currentPtr[2] == '\r'
            && _currentPtr[3] == '\n');
        if (isEmptyLine) {
            _currentPtr += 4;
            return true;
        }

        _currentPtr++;
    }

    return false;
}
