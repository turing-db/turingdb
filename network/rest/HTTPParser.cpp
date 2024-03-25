#include "HTTPParser.h"

#include <iostream>

using namespace net;

static constexpr size_t MIN_METHOD_SIZE = 3;

namespace {

bool isBlank(char c) {
    return (c == ' ') || (c == '\n') || (c == '\r') || (c == '\t');
}

bool isURIValid(char c) {
    return (c == '/') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
        || (c >= '0' && c <= '9')
        || (c == '_' || c == '=' || c == '&' || c == ';' || c == '?');
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

    _payload = std::string_view(_currentPtr, getSize());

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

    auto& uri = _params._uri;
    uri.clear();
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

        uri.push_back(c);
    }

    if (uri.empty()) {
        return false;
    }

    // Extract the path part of the URI
    // up to the ? character if any
    const char* pathBegin = uri.c_str();
    const char* pathPtr = pathBegin;
    const char* const uriEnd = pathPtr+uri.size();
    for (; pathPtr < uriEnd; pathPtr++) {
        if (*pathPtr == '?') {
            break;
        }
    }

    _params._path = std::string_view(pathBegin, pathPtr-pathBegin);
    std::cout << "path=" << _params._path << "\n";
    std::cout << "uri=" << _params._uri << "\n";
    
    // We can stop here if we are already at the end of the URI
    if (pathPtr >= uriEnd) {
        return true;
    }

    // URI variables
    pathPtr++;
    auto& parameters = _params._params;
    std::string_view key;
    std::string_view value;
    const char* wordStart = pathPtr;
    for (; pathPtr < uriEnd; pathPtr++) {
        const char c = *pathPtr;
        if (c == '=') {
            key = std::string_view(wordStart, pathPtr-wordStart);
            value = std::string_view();
            wordStart = pathPtr+1;
        } else if (c == '&') {
            value = std::string_view(wordStart, pathPtr-wordStart);
            if (!key.empty() && !value.empty()) {
                parameters.emplace_back(key, value);
            }

            key = std::string_view();
            value = std::string_view();
            wordStart = pathPtr+1;
        }
    }

    if (wordStart < uriEnd && !key.empty()) {
        value = std::string_view(wordStart, uriEnd-wordStart);
        parameters.emplace_back(key, value);
    }

    return true;
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
