#pragma once

#include <string_view>

#include "AbstractTCPParser.h"
#include "NetBuffer.h"
#include "TuringProtoHeaders.h"

namespace net::proto {

class TuringProtoWriter;

class TuringProtoParser : public AbstractTCPParser {
public:
    enum class Error : int32_t {
        REQUEST_TOO_BIG = 0,
        INVALID_MESSAGE_TYPE,
        INVALID_HEADER
    };

    explicit TuringProtoParser(NetBuffer* inputBuffer);

    [[nodiscard]] AnalyzeResult analyze() override;
    void handleAnalyzeError(AnalyzeError error, AbstractTCPWriter& writer) override;
    void reset() override;

    [[nodiscard]] const ProtoHeader& getHeader() const { return _header; }
    [[nodiscard]] std::string_view getPayload() const { return _payload; }

private:
    NetBuffer::Reader _reader;
    ProtoHeader _header {};
    std::string_view _payload;
    bool _parsedHeader {false};
};

}
