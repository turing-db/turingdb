#pragma once

#include <functional>
#include <memory>

#include "AbstractTCPWriter.h"
#include "BasicResult.h"

namespace net {

class NetBuffer;
class BaseConnectionState;

class AbstractTCPParser {
public:
    using Finished = bool;
    using AnalyzeError = int32_t;
    using AnalyzeResult = BasicResult<Finished, AnalyzeError>;

    virtual ~AbstractTCPParser() = default;

    AbstractTCPParser(const AbstractTCPParser&) = delete;
    AbstractTCPParser(AbstractTCPParser&&) = delete;
    AbstractTCPParser& operator=(const AbstractTCPParser&) = delete;
    AbstractTCPParser& operator=(AbstractTCPParser&&) = delete;

    virtual void reset() = 0;
    [[nodiscard]] virtual AnalyzeResult analyze() = 0;
    virtual void handleAnalyzeError(AnalyzeError error, AbstractTCPWriter& writer) = 0;

protected:
    AbstractTCPParser() = default;
};

using CreateAbstractTCPParserFunc = std::function<
    std::unique_ptr<AbstractTCPParser>(NetBuffer* inputBuffer,
                                       BaseConnectionState* state)>;

}
