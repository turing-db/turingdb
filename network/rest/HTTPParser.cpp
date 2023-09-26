#include "HTTPParser.h"

using namespace net;

static constexpr size_t MIN_METHOD_SIZE = 3;

namespace {

bool isblank(char c) {
    return (c == ' ') || (c == '\n') || (c == '\r') || (c == '\t');
}

bool isvalid(char c) {
    return (c == '/') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
        || (c >= '0' && c <= '9') || (c == '_' || c == '=' || c == '&' || c == ';');
}

}

HTTPParser::HTTPParser(const Buffer* inputBuffer)
    : _reader(inputBuffer->getReader()),
    _currentPtr(_reader.getData())
{
}

bool HTTPParser::analyze() {
    if (!parseRequestLine()) {
        return false;
    }

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
    if (c == 'G') {
        return parseGET();
    }

    return false;
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
    return isGET;
}

bool HTTPParser::parseURI() {
    if (getSize() == 0) {
        return false;
    }

    _uri.clear();
    const char* endPtr = getEndPtr();
    for (; _currentPtr < endPtr; _currentPtr++) {
        const char c = *_currentPtr;
        if (isblank(c)) {
            break;
        }
        if (!isvalid(c)) {
            return false;
        }

        _uri.push_back(c);
    }

    return !_uri.empty();
}
