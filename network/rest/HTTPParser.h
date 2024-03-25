#pragma once

#include <string_view>

#include "Buffer.h"
#include "HTTPParams.h"

namespace net {

class HTTPParser {
public:
    enum class HTTPMethod {
        UNKNOWN,
        GET,
        POST
    };

    explicit HTTPParser(Buffer* inputBuffer);

    bool analyze();
    
    const HTTPParams& getParams() const { return _params; }

    std::string_view getPayload() const { return _payload; }

private:
    Buffer::Reader _reader;
    char* _currentPtr {nullptr};
    HTTPMethod _method {HTTPMethod::UNKNOWN};
    HTTPParams _params;
    std::string_view _payload;

    size_t getSize() { return getEndPtr()-_currentPtr; }
    char* getEndPtr() { return _reader.getData() + _reader.getSize(); }

    bool parseRequestLine();
    bool parseMethod();
    bool parseGET();
    bool parsePOST();
    bool parseURI();
    bool jumpToPayload();
};

}
