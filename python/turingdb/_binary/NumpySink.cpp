#include "NumpySink.h"

#include "ProtoDecodeSink.h"

using namespace pybindings;

static_assert(net::proto::ProtoDecodeSink<NumpySink>);

NumpySink::NumpySink(db::LocalMemory* localMemory,
                     net::proto::ChunkedBuffer<float>* embeddingBuffer,
                     db::ListBuffer<>* listBuffer,
                     db::MapBuffer<>* mapBuffer)
    : TuringSink(localMemory, embeddingBuffer, listBuffer, mapBuffer)
{
}

NumpySink::~NumpySink() {
}
