#pragma once

#include <functional>
#include <memory>

namespace net {

class BaseConnectionState;

class AbstractTCPWriter {
public:
    virtual ~AbstractTCPWriter() = default;

    virtual void flush() = 0;
    virtual void reset() = 0;
    virtual void setSocket(int socket) = 0;
    [[nodiscard]] virtual size_t getBytesWritten() const = 0;
    [[nodiscard]] virtual bool wroteNonEmptyChunk() const = 0;
    [[nodiscard]] virtual bool errorOccured() const = 0;
};

using CreateAbstractTCPWriterFunc = std::function<
    std::unique_ptr<AbstractTCPWriter>(BaseConnectionState* state)>;

}
