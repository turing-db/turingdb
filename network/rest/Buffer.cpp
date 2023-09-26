#include "Buffer.h"

#include <iostream>

using namespace turing::network;

Buffer::Buffer()
{
}

Buffer::~Buffer() {
}

void Buffer::Reader::dump() const {
    std::cout << "Buffer content='";
    std::cout.write(getData(), getSize());
    std::cout << "'\n";
}
