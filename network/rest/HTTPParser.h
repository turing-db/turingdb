#pragma once

#include <string>

#include "Buffer.h"
#include "StringSpan.h"

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
    
    const std::string& getURI() const { return _uri; }

    StringSpan getPayload() const { return _payload; }

private:
    Buffer::Reader _reader;
    char* _currentPtr {nullptr};
    HTTPMethod _method {HTTPMethod::UNKNOWN};
    std::string _uri;
    StringSpan _payload;

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
