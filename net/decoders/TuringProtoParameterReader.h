#pragma once

namespace db {
class ParameterMap;
}

namespace net::proto {

class TuringProtoInBuf;

// Reads one map value of the nested container format into the parameters. The buffer must hold
// the whole map: a map that ends early is malformed, not waiting for more bytes.
void readParameters(TuringProtoInBuf* inBuf, db::ParameterMap& parameters);

}
