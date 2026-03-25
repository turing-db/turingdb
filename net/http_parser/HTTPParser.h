#pragma once

#include "HTTPParsingInfo.h"
#include "AbstractTCPParser.h"
#include "UriParser.h"
#include "NetBuffer.h"
#include "HTTPWriter.h"

namespace net {

template <std::derived_from<URIParser> URIParserT>
class HTTPParser : public AbstractTCPParser {
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
    [[nodiscard]] AnalyzeResult analyze() override {
        const auto res = analyzeHTTP();
        if (!res) {
            return BadResult(static_cast<AnalyzeError>(res.error()));
        }

        return res.value();
    }

    void handleAnalyzeError(AnalyzeError error, AbstractTCPWriter& writer) override {
        handleAnalyzeError(error, static_cast<HTTPWriter&>(writer));
    }

    void handleAnalyzeError(AnalyzeError error, HTTPWriter& httpWriter) {
        switch (static_cast<HTTP::Error>(error)) {
            case net::HTTP::Error::REQUEST_TOO_BIG:
                httpWriter.setFirstLine(net::HTTP::Status::CONTENT_TOO_LARGE);
            break;

            case net::HTTP::Error::HEADER_INCOMPLETE:
                httpWriter.setFirstLine(net::HTTP::Status::BAD_REQUEST);
            break;

            case net::HTTP::Error::TOO_MANY_PARAMS:
                httpWriter.setFirstLine(net::HTTP::Status::CONTENT_TOO_LARGE);
            break;

            case net::HTTP::Error::UNKNOWN_ENDPOINT:
                httpWriter.setFirstLine(net::HTTP::Status::NOT_FOUND);
            break;

            case net::HTTP::Error::INVALID_METHOD:
                httpWriter.setFirstLine(net::HTTP::Status::METHOD_NOT_ALLOWED);
            break;

            case net::HTTP::Error::NO_METHOD:
            case net::HTTP::Error::NO_URI:
            case net::HTTP::Error::UNKNOWN:
            case net::HTTP::Error::INVALID_URI:
            case net::HTTP::Error::_SIZE:
                httpWriter.setFirstLine(net::HTTP::Status::BAD_REQUEST);
            break;
        }

        httpWriter.addConnection(net::getConnectionHeader(true));
        httpWriter.addChunkedTransferEncoding();
        httpWriter.addContentType(net::ContentType::JSON);
        httpWriter.flushHeader();
        httpWriter.flush();
    }

    [[nodiscard]] const HTTP::Info& getHttpInfo() const { return _info; }

    void reset() override {
        _info.reset();
        _currentPtr = _reader.getData();
        _payloadSize = 0;
        _payloadBegin = nullptr;
        _parsedHeader = false;
    }

private:
    HTTP::Info _info;
    NetBuffer::Reader _reader;
    char* _currentPtr {nullptr};
    char* _payloadBegin {nullptr};
    uint64_t _payloadSize {0};
    bool _parsedHeader {false};

    [[nodiscard]] HTTP::Result<Finished> analyzeHTTP() {
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

        if (!finished && _reader.getSize() == NetBuffer::BUFFER_SIZE) {
            return BadResult(HTTP::Error::REQUEST_TOO_BIG);
        }

        return finished;
    }

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

        // Skip optional whitespace after "content-length:"
        while (_currentPtr != endPtr) {
            const char c = *_currentPtr;
            if (c != ' ' && c != '\t') {
                break;
            }
            _currentPtr++;
        }

        // Parse digits with bounds checking (no strtoull — buffer is not null-terminated)
        _payloadSize = 0;
        while (_currentPtr != endPtr) {
            const char c = *_currentPtr;
            if (!isdigit(c)) {
                break;
            }

            // Uses Horner's rule for decimal number parsing
            // (if you don't know what it is please read about it)
            //
            // Arithmetic overflow: we first check that _payloadSize will not exceed BUFFER_SIZE
            // if multiplied by 10 and therefore will not overflow as BUFFER_SIZE is way smaller
            // than the bound
            if (_payloadSize > NetBuffer::BUFFER_SIZE / 10) {
                return BadResult(HTTP::Error::REQUEST_TOO_BIG);
            }

            // We know that c is a digit between '0' and '9' here
            // because we checked isdigit earlier
            _payloadSize = _payloadSize * 10 + (c - '0');

            _currentPtr++;
        }

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
