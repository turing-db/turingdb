#pragma once

#include "dataframe/Dataframe.h"
#include "OutputEncoder.h"

#include "BioAssert.h"

namespace db {

template <Encoder E>
class JsonOutputCallback {
public:
    using EncoderType = E;

    JsonOutputCallback(EncoderType& encoder)
        : _encoder(encoder)
    {
    }

    void writeHeader(const Dataframe* df) {
        bioassert(df != nullptr, "Dataframe is null");
        _encoder.writeDataframeHeader(*df);
    }

    void operator()(const Dataframe* df) {
        bioassert(df != nullptr, "Dataframe is null");
        _encoder.writeDataframe(*df);
    }

private:
    EncoderType& _encoder;
    bool _first {true};
};

}
