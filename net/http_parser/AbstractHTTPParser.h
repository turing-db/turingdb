#pragma once

#include <functional>
#include <memory>

#include "HTTPParsingInfo.h"

namespace net {

class NetBuffer;

class AbstractHTTPParser {
public:
    using Finished = bool;

    virtual ~AbstractHTTPParser() = default;

    AbstractHTTPParser(const AbstractHTTPParser&) = delete;
    AbstractHTTPParser(AbstractHTTPParser&&) = delete;
    AbstractHTTPParser& operator=(const AbstractHTTPParser&) = delete;
    AbstractHTTPParser& operator=(AbstractHTTPParser&&) = delete;

    virtual void reset() {
        _info.reset();
    };

    [[nodiscard]] virtual HTTP::Result<Finished> analyze() = 0;

    const HTTP::Info& getHttpInfo() const { return _info; }

protected:
    HTTP::Info _info;

    AbstractHTTPParser() = default;
};

using  CreateAbstractHTTPParserFunc = std::function<std::unique_ptr<AbstractHTTPParser>(NetBuffer* inputBuffer)>;

}
