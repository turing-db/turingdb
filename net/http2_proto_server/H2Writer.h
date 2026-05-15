#pragma once

#include <stddef.h>

#include "AbstractTCPWriter.h"

namespace net::H2 {

class H2ConnectionState;

class H2Writer : public net::AbstractTCPWriter {
public:
    explicit H2Writer(BaseConnectionState* state);
    ~H2Writer() override;

    H2Writer() = delete;
    H2Writer(const H2Writer&) = delete;
    H2Writer(H2Writer&&) = delete;
    H2Writer& operator=(const H2Writer&) = delete;
    H2Writer& operator=(H2Writer&&) = delete;

    void flush() override;
    void reset() override;
    void setSocket(int socket) override;

    [[nodiscard]] size_t getBytesWritten() const override;
    [[nodiscard]] bool wroteNonEmptyChunk() const override;
    [[nodiscard]] bool errorOccured() const override;

private:
    H2ConnectionState* _connectionState {nullptr};
    int _socket {-1};
    bool _errorOccurred {false};
};

}
