#include "TuringProtoParameterReader.h"

#include "DecodeContext.h"
#include "Decoders.h"
#include "ParameterMapSink.h"
#include "ProtocolException.h"
#include "map/MapView.h"

using namespace net::proto;

void net::proto::readParameters(TuringProtoInBuf* inBuf, db::ParameterMap& parameters) {
    DecodeContext context;
    context._inBuf = inBuf;

    ParameterMapSink sink(&parameters);
    ParameterMapSink::ColumnConst<db::MapView> root;

    const bool complete = ConstColumnDecoder<db::MapView, ParameterMapSink>::decode(&context, &sink, &root);
    if (!complete) {
        throw ProtocolException("The parameter map ends before its last value");
    }
}
