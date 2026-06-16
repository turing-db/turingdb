#pragma once

#include "HTTPParsingInfo.h"
#include "AbstractTCPParser.h"
#include "UriParser.h"
#include "NetBuffer.h"
#include "HTTPWriter.h"
#include "HTTPUtils.h"

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
        _contentLengthSeen = false;
    }

private:
    HTTP::Info _info;
    NetBuffer::Reader _reader;
    char* _currentPtr {nullptr};
    char* _payloadBegin {nullptr};
    uint64_t _payloadSize {0};
    bool _parsedHeader {false};
    bool _contentLengthSeen {false};

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

            if (auto res = parseHeaders(); !res) {
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

    // Walks the header block exactly once. For each header line it parses the
    // name and value, folds content-length into _payloadSize, captures the
    // Authorization header into Info, and leaves _currentPtr at the start of
    // the payload. The full header block must be present in the buffer (it is
    // received in one chunk); a missing terminating blank line is reported as
    // HEADER_INCOMPLETE.
    [[nodiscard]] HTTP::Result<void> parseHeaders() {
        const char* const endPtr = getEndPtr();

        // Skip the remainder of the request line, up to and including its CRLF.
        while (_currentPtr < endPtr && *_currentPtr != '\n') {
            _currentPtr++;
        }

        if (_currentPtr < endPtr) {
            _currentPtr++;
        }

        while (_currentPtr < endPtr) {
            // A blank line (CRLF at a line start) terminates the header block;
            // the payload begins immediately after it.
            const bool isBlankLine = (endPtr - _currentPtr) >= 2
                                     && _currentPtr[0] == '\r'
                                     && _currentPtr[1] == '\n';
            if (isBlankLine) {
                _currentPtr += 2;
                return {};
            }

            // Header name: up to the ':'.
            const char* nameBegin = _currentPtr;
            while (_currentPtr < endPtr && *_currentPtr != ':' && *_currentPtr != '\r' && *_currentPtr != '\n') {
                _currentPtr++;
            }

            if (_currentPtr >= endPtr || *_currentPtr != ':') {
                return BadResult(HTTP::Error::HEADER_INCOMPLETE);
            }

            const std::string_view name(nameBegin, _currentPtr - nameBegin);
            _currentPtr++;

            // Skip optional whitespace, then read the value to the end of line.
            while (_currentPtr < endPtr && (*_currentPtr == ' ' || *_currentPtr == '\t')) {
                _currentPtr++;
            }

            const char* valueBegin = _currentPtr;
            while (_currentPtr < endPtr && *_currentPtr != '\r' && *_currentPtr != '\n') {
                _currentPtr++;
            }

            const std::string_view value(valueBegin, _currentPtr - valueBegin);

            if (auto res = captureHeader(name, value); !res) {
                return res.get_unexpected();
            }

            // Advance to the start of the next line.
            while (_currentPtr < endPtr && *_currentPtr != '\n') {
                _currentPtr++;
            }

            if (_currentPtr < endPtr) {
                _currentPtr++;
            }
        }

        return BadResult(HTTP::Error::HEADER_INCOMPLETE);
    }

    [[nodiscard]] HTTP::Result<void> captureHeader(std::string_view name, std::string_view value) {
        if (net::http::equalsIgnoreCaseAscii(name, "content-length")) {
            // First Content-Length wins. A conforming request carries at most
            // one; ignoring any later duplicate keeps a malformed request (e.g.
            // two differing lengths) from overwriting _payloadSize with a value
            // larger than the body, which would otherwise leave the parser
            // blocked waiting for bytes that never arrive.
            if (_contentLengthSeen) {
                return {};
            }

            _contentLengthSeen = true;

            // Parse digits with bounds checking (no strtoull — buffer is not
            // null-terminated). Horner's rule; the overflow guard keeps
            // _payloadSize * 10 well below the size_t bound because BUFFER_SIZE
            // is far smaller.
            _payloadSize = 0;
            for (const char c : value) {
                if (!isdigit(static_cast<unsigned char>(c))) {
                    break;
                }

                if (_payloadSize > NetBuffer::BUFFER_SIZE / 10) {
                    return BadResult(HTTP::Error::REQUEST_TOO_BIG);
                }

                _payloadSize = _payloadSize * 10 + (c - '0');
            }

            return {};
        } else if (net::http::equalsIgnoreCaseAscii(name, "authorization")) {
            _info._authorization = value;
        }

        return {};
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
