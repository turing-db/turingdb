#include "NetBuffer.h"

#include <iostream>

using namespace net;

NetBuffer::NetBuffer()
{
}

NetBuffer::~NetBuffer() {
}

void NetBuffer::Reader::dump() const {
    std::cout << "Buffer content='";
    std::cout.write(getData(), getSize());
    std::cout << "'\n";
}
