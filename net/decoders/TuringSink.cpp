#include "TuringSink.h"

using namespace net::proto;

TuringSink::TuringSink(db::LocalMemory* localMemory, TuringValueStore* values)
    : _localMemory(localMemory),
    _values(values)
{
}

TuringSink::~TuringSink() {
}
