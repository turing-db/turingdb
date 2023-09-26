#pragma once

#include <string>

#include "Buffer.h"

namespace turing::network {

class Buffer;

class HTTPParser {
public:
    enum class HTTPMethod {
        UNKNOWN,
        GET,
        POST
    };

    explicit HTTPParser(const Buffer* inputBuffer);

    bool analyze();
    
    const std::string& getURI() const { return _uri; }

private:
    Buffer::Reader _reader;
    const char* _currentPtr {nullptr};
    HTTPMethod _method {HTTPMethod::UNKNOWN};
    std::string _uri;

    size_t getSize() const { return _reader.getSize(); }
    const char* getEndPtr() const { return _reader.getData() + _reader.getSize(); }

    bool parseRequestLine();
    bool parseMethod();
    bool parseGET();
    bool parseURI();
};

}
