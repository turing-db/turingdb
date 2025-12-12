#include "FileReader.h"

using namespace fs;

FileReader::FileReader()
{
}

FileReader::~FileReader() {
}

void FileReader::read() {
    const size_t size = _file->getInfo()._size;

    if (size == 0) {
        _buffer.clear();
        return;
    }

    _buffer.resize(size);

    auto res = _file->read(_buffer.data(), _buffer.size());
    if (!res) {
        _error = res.error();
    }
}
