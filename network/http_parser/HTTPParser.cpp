#include "HTTPParser.h"

#include <cctype>
#include <cstdlib>

using namespace net;

namespace {

}

HTTPParser::HTTPParser(Buffer* inputBuffer)
    : _reader(inputBuffer->getReader()),
      _currentPtr(_reader.getData())
{
}

void HTTPParser::reset() {
    _currentPtr = _reader.getData();
    _payloadSize = 0;
    _payloadBegin = nullptr;
    _parsedHeader = false;
    _info.reset();
}
