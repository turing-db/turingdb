#pragma once

#include <functional>
#include <vector>

#include "DecodedColumnSchema.h"
#include "ProtoDecodeSink.h"
#include "TuringProtoDecoder.h"
#include "TuringProtoInBuf.h"
#include "TuringResponseConsumer.h"

namespace net::proto {

// Decodes the result packets of a response into the columns of any sink. Each result chunk
// is handed over once its END_CHUNK has been decoded, and a response with no chunk hands
// over one of no rows, so the columns the header declared are seen either way.
template <ProtoDecodeSink Sink>
class TuringResponseDecoder : public TuringResponseConsumer {
public:
    using OnChunkDecoded = std::function<void()>;

    TuringResponseDecoder(TuringProtoInBuf* inBuf,
                          Sink* sink,
                          SinkColumnContainer<Sink>* container,
                          const OnChunkDecoded& onChunkDecoded);
    ~TuringResponseDecoder() override;

    void onChunkHeader() override;
    void onChunk() override;
    void onEndChunk() override;
    void onEnd() override;

private:
    std::vector<DecodedColumnSchema> _columnSchemas;
    TuringProtoDecoder<Sink> _decoder;
    SinkColumnContainer<Sink>* _container {nullptr};
    OnChunkDecoded _onChunkDecoded;
    bool _chunkDecoded {false};
};

template <ProtoDecodeSink Sink>
TuringResponseDecoder<Sink>::TuringResponseDecoder(TuringProtoInBuf* inBuf,
                                                   Sink* sink,
                                                   SinkColumnContainer<Sink>* container,
                                                   const OnChunkDecoded& onChunkDecoded)
    : _decoder(inBuf, sink, _columnSchemas),
    _container(container),
    _onChunkDecoded(onChunkDecoded)
{
}

template <ProtoDecodeSink Sink>
TuringResponseDecoder<Sink>::~TuringResponseDecoder() {
    _decoder.reset();
}

template <ProtoDecodeSink Sink>
void TuringResponseDecoder<Sink>::onChunkHeader() {
    _decoder.decodeIncomingChunkHeader(_container);
}

template <ProtoDecodeSink Sink>
void TuringResponseDecoder<Sink>::onChunk() {
    _decoder.decodeIncomingChunk(_container);
}

template <ProtoDecodeSink Sink>
void TuringResponseDecoder<Sink>::onEndChunk() {
    _decoder.decodeChunkFooter(_container);

    _chunkDecoded = true;
    _onChunkDecoded();

    for (DecodedColumnSchema& schema : _columnSchemas) {
        schema.getColumnState().reset();
    }
    _decoder.reset();
}

template <ProtoDecodeSink Sink>
void TuringResponseDecoder<Sink>::onEnd() {
    if (_chunkDecoded) {
        return;
    }

    _container->setRowCount(0);
    _onChunkDecoded();
}

}
