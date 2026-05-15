#pragma once

#include "AbstractTCPParser.h"

namespace net {
class NetBuffer;
}

namespace net::H2 {

class H2ConnectionState;

class H2Parser : public net::AbstractTCPParser {
public:
    H2Parser(NetBuffer* inputBuffer, BaseConnectionState* state);
    ~H2Parser() override;

    H2Parser() = delete;
    H2Parser(const H2Parser&) = delete;
    H2Parser(H2Parser&&) = delete;
    H2Parser& operator=(const H2Parser&) = delete;
    H2Parser& operator=(H2Parser&&) = delete;

    [[nodiscard]] AnalyzeResult analyze() override;
    void handleAnalyzeError(AnalyzeError error, AbstractTCPWriter& writer) override;
    void reset() override;

private:
    NetBuffer* _inputBuffer {nullptr};
    H2ConnectionState* _connectionState {nullptr};
};

}
