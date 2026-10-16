#pragma once

#include "TuringProtoNestedWriter.h"
#include "list/ListBuffer.h"
#include "map/MapBuffer.h"

namespace db {
class ParameterMap;
}

namespace net::proto {

class TuringProtoOutBuf;

// Writes a parameter map as one map value of the nested container format: the names are the
// keys, sorted, and each value keeps its type.
class TuringProtoParameterWriter {
public:
    explicit TuringProtoParameterWriter(TuringProtoOutBuf* outBuf);
    ~TuringProtoParameterWriter();

    void writeParameters(const db::ParameterMap& parameters);

private:
    db::QueryListBuffer _listBuffer;
    db::MapBuffer<> _mapBuffer;
    NestedContainerWriter _writer;
};

}
