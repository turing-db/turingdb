#pragma once

#include "HTTPParsingInfo.h"
#include "AbstractHTTPParser.h"
#include "UriParser.h"
#include "Endpoints.h"
#include "NetBuffer.h"

namespace net {

template <std::derived_from<URIParser> URIParserT>
class HTTPParser : public AbstractHTTPParser {
public:
    explicit HTTPParser(NetBuffer* inputBuffer)
        : _reader(inputBuffer->getReader()),
        _currentPtr(_reader.getData())
    {
    }

    /* @brief Analyze the incoming data.
     *
     * The HTTP header must be received in one chunk.
     * Only the payload is allowed to be received in multiple chunks
     * */
    [[nodiscard]] HTTP::Result<Finished> analyze() override {
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

            if (auto res = parseRequiredHeadersAndJump(); !res) {
                return res.get_unexpected();
            }

            _parsedHeader = true;
        }

        if (!_payloadBegin) {
            _payloadBegin = _currentPtr;
        }

        _info._payload = std::string_view {_payloadBegin, getSize()};
        const bool finished = _info._payload.size() == _payloadSize;

        if (!finished && _reader.getSize() == NetBuffer::BUFFER_SIZE) {
            return BadResult(HTTP::Error::REQUEST_TOO_BIG);
        }

        return finished;
    }

    const std::string& getPayLoadType() {
        return _payloadType;
    }

    void reset() override {
        AbstractHTTPParser::reset();
        _currentPtr = _reader.getData();
        _payloadSize = 0;
        _payloadBegin = nullptr;
        _parsedHeader = false;
    }

private:
    NetBuffer::Reader _reader;
    char* _currentPtr {nullptr};
    char* _payloadBegin {nullptr};
    uint64_t _payloadSize {};
    std::string _payloadType;
    bool _parsedHeader = false;

    size_t getSize() { return getEndPtr() - _currentPtr; }
    char* getEndPtr() { return _reader.getData() + _reader.getSize(); }

    [[nodiscard]] HTTP::Result<void> parseMethod() {
        if (getSize() < 5) {
            return BadResult(HTTP::Error::NO_METHOD);
        }

        const char c = *_currentPtr;
        if (c == 'P' || c == 'p') {
            return parsePOST();
        }

        if (c == 'G' || c == 'g') {
            return parseGET();
        }

        return BadResult(HTTP::Error::INVALID_METHOD);
    }

    [[nodiscard]] HTTP::Result<void> parsePOST() {
        const bool isPOST = (_currentPtr[1] == 'O' || _currentPtr[1] == 'o')
                         && (_currentPtr[2] == 'S' || _currentPtr[2] == 's')
                         && (_currentPtr[3] == 'T' || _currentPtr[3] == 't');

        if (!isPOST) {
            return BadResult(HTTP::Error::INVALID_METHOD);
        }

        _info._method = HTTP::Method::POST;
        _currentPtr += 4;

        return {};
    }

    [[nodiscard]] HTTP::Result<void> parseGET() {
        const bool isGET = (_currentPtr[1] == 'E' || _currentPtr[1] == 'e')
                        && (_currentPtr[2] == 'T' || _currentPtr[2] == 't');

        if (!isGET) {
            return BadResult(HTTP::Error::INVALID_METHOD);
        }

        _info._method = HTTP::Method::GET;
        _currentPtr += 3;

        return {};
    }

    [[nodiscard]] HTTP::Result<void> parseURI() {
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
            return BadResult(HTTP::Error::NO_URI);
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
                return BadResult(HTTP::Error::INVALID_URI);
            }
        }

        if (!foundEnd) {
            return BadResult(HTTP::Error::HEADER_INCOMPLETE);
        }

        uri = std::string_view(beginPtr, _currentPtr);

        if (uri.empty()) {
            return BadResult(HTTP::Error::INVALID_URI);
        }

        return URIParserT::parseURI(_info, uri);
    }

    [[nodiscard]] HTTP::Result<void> parseRequiredHeadersAndJump() {
        const char* endPtr = getEndPtr();
        const std::string_view contentLengthKey = "content-length:";
        const std::string_view contentTypeKey = "content-type:";
        uint8_t headerFlags = 0;
        std::string_view contentLengthWindow;
        std::string_view contentTypeWindow;

        char* contentLenPtr = nullptr;
        char* contentTypePtr = nullptr;

        for (; headerFlags != 3 && _currentPtr != endPtr; _currentPtr++) {
            if (getSize() < contentLengthKey.size()) {
                return jumpToPayload();
            }

            contentLengthWindow = {_currentPtr, _currentPtr + contentLengthKey.size()};
            contentTypeWindow = {_currentPtr, _currentPtr + contentTypeKey.size()};

            if (std::equal(contentLengthKey.begin(), contentLengthKey.end(),
                           contentLengthWindow.begin(), contentLengthWindow.end(),
                           [](char a, char b) {
                               return a == tolower(b);
                           })) {
                headerFlags |= 1;
                contentLenPtr = _currentPtr;
            }

            if (std::equal(contentTypeKey.begin(), contentTypeKey.end(),
                           contentTypeWindow.begin(), contentTypeWindow.end(),
                           [](char a, char b) {
                               return a == tolower(b);
                           })) {
                headerFlags |= 2;
                contentTypePtr = _currentPtr;
            }

            const bool isEmptyLine = (_currentPtr[0] == '\r'
                                      && _currentPtr[1] == '\n'
                                      && _currentPtr[2] == '\r'
                                      && _currentPtr[3] == '\n');
            if (isEmptyLine) {
                return jumpToPayload();
            }
        }

        contentLenPtr += contentLengthKey.size();
        const char* lengthBegin = contentLenPtr;

        for (; contentLenPtr != endPtr; contentLenPtr++) {
            if (isdigit(*contentLenPtr)) {
                lengthBegin = contentLenPtr;
                break;
            }
        }
        _payloadSize = std::strtoull(lengthBegin, &_currentPtr, 10);

        contentTypePtr += contentTypeKey.size();
        const char* typeBegin = contentTypePtr;

        for (; contentTypePtr != endPtr; contentLenPtr++) {
            if (!isBlank(*contentTypePtr)) {
                typeBegin = contentTypePtr;
                break;
            }
        }

        while (contentTypePtr != endPtr && *contentTypePtr != '\r') {
            contentTypePtr++;
        }
        _payloadType = std::string(typeBegin, (contentTypePtr - typeBegin));

        return jumpToPayload();
    }

    [[nodiscard]] HTTP::Result<void> parseContentLengthAndJump() {
        const char* endPtr = getEndPtr();
        const std::string_view key = "content-length:";
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
                return jumpToPayload();
            }
        }

        _currentPtr += key.size();
        const char* lengthBegin = _currentPtr;

        for (; _currentPtr != endPtr; _currentPtr++) {
            if (!isdigit(*_currentPtr)) {
                lengthBegin = _currentPtr;
                break;
            }
        }

        _payloadSize = std::strtoull(lengthBegin, &_currentPtr, 10);
        return jumpToPayload();
    }

    [[nodiscard]] HTTP::Result<void> jumpToPayload() {
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

        return BadResult(HTTP::Error::HEADER_INCOMPLETE);
    }


    static bool isBlank(char c) {
        return (c == ' ') || (c == '\n') || (c == '\r') || (c == '\t');
    }

    static bool isURIValid(char c) {
        return (c == '/') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
            || (c >= '0' && c <= '9')
            || (c == '_' || c == '=' || c == '&' || c == ';' || c == '?' || c == '-' || c == '.');
    }
};
}
