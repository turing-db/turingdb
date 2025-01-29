#include "JsonEncoderStep.h"

#include "NetWriter.h"
#include "JsonEncodingUtils.h"

using namespace db;

void JsonEncoderStep::execute() {
    if (!_first) {
        _writer->write(',');
    }
    JsonEncodingUtils<net::NetWriter>::writeBlock(_block, _writer);
    _first = false;
}
