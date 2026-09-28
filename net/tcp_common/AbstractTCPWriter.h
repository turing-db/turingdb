#pragma once

#include <stdint.h>
#include <functional>
#include <memory>

namespace net {

class AbstractTCPWriter {
public:
    virtual ~AbstractTCPWriter() = default;

    virtual void flush() = 0;
    virtual void reset() = 0;
    virtual void setSocket(int socket) = 0;
    [[nodiscard]] virtual size_t getBytesWritten() const = 0;
    [[nodiscard]] virtual bool wroteNonEmptyChunk() const = 0;
    [[nodiscard]] virtual bool errorOccured() const = 0;

    // Emit an error response for a request that failed protocol analysis, before dispatch.
    // The error is an AbstractTCPParser::AnalyzeError, an HTTP::Error ordinal; each writer
    // renders it in its own wire format. Dispatching on the writer keeps the binary protocol
    // from being handed an HTTP error response, which a static downcast would have produced.
    virtual void writeAnalyzeError(int32_t error) = 0;
};

using CreateAbstractTCPWriterFunc = std::function<std::unique_ptr<AbstractTCPWriter>()>;

}
