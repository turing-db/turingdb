#pragma once

#include <stddef.h>
#include <string>
#include <vector>

#include "Bitmask.h"
#include "DecodeContext.h"
#include "DecodedColumnSchema.h"
#include "TuringProtoHeaders.h"

namespace db {
template <typename T>
class ColumnVector;
template <typename T>
class ColumnConst;

}

namespace net::proto {

class TuringProtoInBuf;

template<typename Sink>
class TuringProtoDecoder {
public:
    TuringProtoDecoder(TuringProtoInBuf* inBuf,
                       Sink* sink,
                       std::vector<DecodedColumnSchema>& colSchema);

    void decodeIncomingChunkHeader(Sink::ColumnContainer* container);
    void decodeIncomingChunk(Sink::ColumnContainer* container);
    void decodeIncomingData(Sink::ColumnContainer* container);

    void reset();

    template <typename T>
    bool decodeColumn(Sink::template ColumnVector<T>* col, DfColumnState* colState);

    template <typename T>
    bool decodeColumn(Sink::template ColumnVector<std::optional<T>>* col, DfColumnState* colState);

    template <typename T>
    bool decodeColumn(Sink::template ColumnConst<T>* col);

    template <typename T>
    bool decodeColumn(Sink::template ColumnConst<std::optional<T>>* col);

private:
    std::vector<DecodedColumnSchema>& _colSchemas;
    DecodeContext _ctxt;
    Sink* _sink {nullptr};
};
}
