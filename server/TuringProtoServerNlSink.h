#pragma once

#include <span>
#include <string_view>

#include "NLOutputSink.h"

namespace net::proto {
class TuringProtoWriter;
}

namespace db {

class TuringProtoServerNlSink : public NLOutputSink {
public:
    explicit TuringProtoServerNlSink(net::proto::TuringProtoWriter* writer);
    ~TuringProtoServerNlSink() override;

    void declareOutput(std::span<const std::string_view> names,
                       std::span<const Column* const> chunks) override;
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override;

private:
    net::proto::TuringProtoWriter* _writer {nullptr};
};

}
