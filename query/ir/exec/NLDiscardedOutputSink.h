#pragma once

#include "NLOutputSink.h"

namespace db {

class NLDiscardedOutputSink : public NLOutputSink {
public:
    NLDiscardedOutputSink();
    ~NLDiscardedOutputSink() override;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override;
};

}
