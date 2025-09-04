#pragma once

#include "HTTPParsingInfo.h"

namespace net {

class NetBuffer;

class AbstractHTTPParser {
public:
    using Finished = uint8_t;

    virtual ~AbstractHTTPParser() = default;

    AbstractHTTPParser(const AbstractHTTPParser&) = delete;
    AbstractHTTPParser(AbstractHTTPParser&&) = delete;
    AbstractHTTPParser& operator=(const AbstractHTTPParser&) = delete;
    AbstractHTTPParser& operator=(AbstractHTTPParser&&) = delete;

    virtual void reset() {
        _info.reset();
    };

    void setHttpBody(char* begin, size_t size) {
        _info._payload = std::string_view {begin, size};
        _info._bytesRead += size;
    }

    [[nodiscard]] virtual HTTP::Result<Finished> analyze() = 0;

    const HTTP::Info& getHttpInfo() const { return _info; }

protected:
    HTTP::Info _info;

    AbstractHTTPParser() = default;
};

using  CreateAbstractHTTPParserFunc = std::function<std::unique_ptr<AbstractHTTPParser>(NetBuffer* inputBuffer)>;

}
