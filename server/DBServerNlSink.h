#pragma once

#include <span>
#include <string_view>

#include "HTTPWriter.h"
#include "JsonEncoder.h"
#include "NLOutputSink.h"

namespace db {

class DBServerNlSink : public NLOutputSink {
public:
    explicit DBServerNlSink(JsonEncoder<net::NetWriter>* encoder);
    ~DBServerNlSink() override;

    void declareOutput(std::span<const std::string_view> names,
                       std::span<const Column* const> chunks) override;
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override;

private:
    JsonEncoder<net::NetWriter>* _encoder {nullptr};
};

}
