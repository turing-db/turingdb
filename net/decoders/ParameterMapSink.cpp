#include "ParameterMapSink.h"

using namespace net::proto;

ParameterMapSink::ParameterMapSink(db::ParameterMap* parameters)
    : _values(parameters)
{
}

ParameterMapSink::~ParameterMapSink() {
}
