#pragma once

#include "BasicResult.h"
#include "Buffer.h"
#include "HTTPParsingInfo.h"

namespace net {

class HTTPParser {
public:
    using Finished = bool;

    enum class Error {
        UNKNOWN = 0,
        HEADER_INCOMPLETE,
        REQUEST_TOO_BIG,
        NO_METHOD,
        NO_URI,
        INVALID_METHOD,
        INVALID_URI,

        _SIZE,
    };

    template <class TValue>
    using Result = BasicResult<TValue, Error>;

    explicit HTTPParser(Buffer* inputBuffer);

    /* @brief Analyze the incoming data.
     *
     * The HTTP header must be received in one chunk.
     * Only the payload is allowed to be received in multiple chunks
     * */
    [[nodiscard]] Result<Finished> analyze() {
        if (getSize() == 0) {
            return true;
        }

        if (!_parsedHeader) {
            if (auto res = parseMethod(); !res) {
                return res.get_unexpected();
            };

            if (auto res = parseURI(); !res) {
                return res.get_unexpected();
            }

            if (auto res = parseContentLengthAndJump(); !res) {
                return res.get_unexpected();
            }

            _parsedHeader = true;
        }

        if (!_payloadBegin) {
            _payloadBegin = _currentPtr;
        }

        _info._payload = std::string_view {_payloadBegin, getSize()};
        const bool finished = _info._payload.size() == _payloadSize;


        if (!finished && _reader.getSize() == Buffer::BUFFER_SIZE) {
            return BadResult(Error::REQUEST_TOO_BIG);
        }

        return finished;
    }

    void reset();

    const HTTP::Info& getHttpInfo() const { return _info; }

private:
    Buffer::Reader _reader;
    char* _currentPtr {nullptr};
    char* _payloadBegin {nullptr};
    uint64_t _payloadSize {};
    bool _parsedHeader = false;
    HTTP::Info _info;

    size_t getSize() { return getEndPtr() - _currentPtr; }
    char* getEndPtr() { return _reader.getData() + _reader.getSize(); }

    [[nodiscard]] Result<void> parseMethod() {
        if (getSize() < 5) {
            return BadResult(Error::NO_METHOD);
        }

        const char c = *_currentPtr;
        if (c == 'P' || c == 'p') {
            return parsePOST();
        }

        if (c == 'G' || c == 'g') {
            return parseGET();
        }

        return BadResult(Error::INVALID_METHOD);
    }

    [[nodiscard]] Result<void> parsePOST() {
        const bool isPOST = (_currentPtr[1] == 'O' || _currentPtr[1] == 'o')
                         && (_currentPtr[2] == 'S' || _currentPtr[2] == 's')
                         && (_currentPtr[3] == 'T' || _currentPtr[3] == 't');

        if (!isPOST) {
            return BadResult(Error::INVALID_METHOD);
        }

        _info._method = HTTP::Method::POST;
        _currentPtr += 4;

        return {};
    }

    [[nodiscard]] Result<void> parseGET() {
        const bool isGET = (_currentPtr[1] == 'E' || _currentPtr[1] == 'e')
                        && (_currentPtr[2] == 'T' || _currentPtr[2] == 't');

        if (!isGET) {
            return BadResult(Error::INVALID_METHOD);
        }

        _info._method = HTTP::Method::GET;
        _currentPtr += 3;

        return {};
    }

    [[nodiscard]] Result<void> parseURI() {
        auto& uri = _info._uri;

        bool foundBegin = false;
        const char* endPtr = getEndPtr();
        for (; _currentPtr < endPtr; _currentPtr++) {
            if (*_currentPtr != ' ') {
                foundBegin = true;
                break;
            }
        }

        if (!foundBegin) {
            return BadResult(Error::NO_URI);
        }

        const char* beginPtr = _currentPtr;

        bool foundEnd = false;
        for (; _currentPtr < endPtr; _currentPtr++) {
            const char c = *_currentPtr;
            if (isBlank(c)) {
                foundEnd = true;
                break;
            }

            if (!isURIValid(c)) {
                return BadResult(Error::INVALID_URI);
            }
        }

        if (!foundEnd) {
            return BadResult(Error::HEADER_INCOMPLETE);
        }

        uri = std::string_view(beginPtr, _currentPtr);

        if (uri.empty()) {
            return BadResult(Error::INVALID_URI);
        }

        // Extract the path part of the URI
        // up to the ? character if any
        const char* pathBegin = uri.data();
        const char* pathPtr = pathBegin;
        const char* const uriEnd = pathPtr + uri.size();
        for (; pathPtr < uriEnd; pathPtr++) {
            if (*pathPtr == '?') {
                break;
            }
        }

        _info._path = std::string_view(pathBegin, pathPtr - pathBegin);

        // We can stop here if we are already at the end of the URI
        if (pathPtr >= uriEnd) {
            return {};
        }

        // URI variables
        pathPtr++;
        auto& parameters = _info._params;
        std::string_view key;
        std::string_view value;
        const char* wordStart = pathPtr;
        for (; pathPtr < uriEnd; pathPtr++) {
            const char c = *pathPtr;
            if (c == '=') {
                key = std::string_view(wordStart, pathPtr - wordStart);
                value = std::string_view();
                wordStart = pathPtr + 1;
            } else if (c == '&') {
                value = std::string_view(wordStart, pathPtr - wordStart);
                if (!key.empty() && !value.empty()) {
                    if (key == "db") {
                        parameters[(size_t)HTTP::Param::db] = value;
                    }
                }

                key = std::string_view();
                value = std::string_view();
                wordStart = pathPtr + 1;
            }
        }

        if (wordStart < uriEnd && !key.empty()) {
            value = std::string_view(wordStart, uriEnd - wordStart);
            if (key == "db") {
                parameters[(size_t)HTTP::Param::db] = value;
            }
        }

        return {};
    }

    [[nodiscard]] Result<void> parseContentLengthAndJump() {
        const char* endPtr = getEndPtr();
        std::string_view key = "content-length:";
        std::string_view window;

        for (; _currentPtr != endPtr; _currentPtr++) {
            if (getSize() < key.size()) {
                return jumpToPayload();
            }

            window = {_currentPtr, _currentPtr + key.size()};

            if (std::equal(key.begin(), key.end(),
                           window.begin(), window.end(),
                           [](char a, char b) {
                               return a == tolower(b);
                           })) {
                break;
            }

            const bool isEmptyLine = (_currentPtr[0] == '\r'
                                      && _currentPtr[1] == '\n'
                                      && _currentPtr[2] == '\r'
                                      && _currentPtr[3] == '\n');
            if (isEmptyLine) {
                continue;
            }
        }

        _currentPtr += key.size();
        const char* _lengthBegin = _currentPtr;

        for (; _currentPtr != endPtr; _currentPtr++) {
            if (!isdigit(*_currentPtr)) {
                _lengthBegin = _currentPtr;
                break;
            }
        }

        _payloadSize = std::strtoull(_lengthBegin, &_currentPtr, 10);
        return jumpToPayload();
    }

    [[nodiscard]] Result<void> jumpToPayload() {
        const char* endPtr = getEndPtr();

        while (endPtr - _currentPtr >= 4) {
            const bool isEmptyLine = (_currentPtr[0] == '\r'
                                      && _currentPtr[1] == '\n'
                                      && _currentPtr[2] == '\r'
                                      && _currentPtr[3] == '\n');
            if (isEmptyLine) {
                _currentPtr += 4;
                return {};
            }

            _currentPtr++;
        }

        return BadResult(Error::HEADER_INCOMPLETE);
    }


    static bool isBlank(char c) {
        return (c == ' ') || (c == '\n') || (c == '\r') || (c == '\t');
    }

    static bool isURIValid(char c) {
        return (c == '/') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
            || (c >= '0' && c <= '9')
            || (c == '_' || c == '=' || c == '&' || c == ';' || c == '?' || c == '-');
    }
};

}
