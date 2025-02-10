#include "FilePageWriter.h"

#include <fcntl.h>

using namespace fs;

Result<FilePageWriter> FilePageWriter::open(const Path& path) {
    const int access = O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT;
    const int permissions = S_IRUSR | S_IWUSR;

    const int fd = ::open(path.c_str(), access, permissions);

    if (fd < 0) {
        return Error::result(ErrorType::OPEN_FILE, errno);
    }

    return FilePageWriter {fd};
}

Result<FilePageWriter> FilePageWriter::openNoDirect(const Path& path) {
    const int access = O_WRONLY | O_TRUNC | O_CREAT;
    const int permissions = S_IRUSR | S_IWUSR;

    const int fd = ::open(path.c_str(), access, permissions);

    if (fd < 0) {
        return Error::result(ErrorType::OPEN_FILE, errno);
    }

    return FilePageWriter {fd};
}

void FilePageWriter::write(const uint8_t* data, size_t size) {
    size_t toWrite = size;
    const uint8_t* curr = data;

    while (toWrite != 0) {
        if (_buffer.avail() == 0) {
            flush();
        }

        const size_t currentSize = std::min(_buffer.avail(), toWrite);
        _buffer.write(curr, currentSize);
        toWrite -= currentSize;
        curr += currentSize;
        _written += currentSize;
    }
}

void FilePageWriter::writeToCurrentPage(std::span<const uint8_t> data) {
    bioassert(_buffer.avail() >= data.size());

    const size_t size = data.size();

    _buffer.write(data.data(), size);
    _written += size;
}

void FilePageWriter::sync() {
    if (auto res = ::fsync(_fd); res != 0) {
        _error = Error(ErrorType::SYNC_FILE, errno);
    }
}

void FilePageWriter::finish() {
    if (_buffer.size() == 0) {
        return;
    }

    if (_buffer.size() != InternalBuffer::Capacity) {
        std::memset(_buffer.data() + _buffer.size(), 0, _buffer.avail());
    }

    flush();
    sync();
}

void FilePageWriter::nextPage() {
    if (_buffer.size() != InternalBuffer::Capacity) {
        std::memset(_buffer.data() + _buffer.size(), 0, _buffer.avail());
    }

    flush();
}

void FilePageWriter::seek(size_t offset) {
    flush();

    if (int res = ::lseek(_fd, offset, SEEK_SET); res < 0) {
        _error = Error(ErrorType::COULD_NOT_SEEK, errno);
    }

    _written = 0;
}

void FilePageWriter::reserveSpace(size_t byteCount) {
    _buffer.reserveSpace(byteCount);
}

void FilePageWriter::flush() {
    ssize_t remainingBytes = PAGE_SIZE;

    while (remainingBytes > 0) {
        const ssize_t nbytes = ::write(_fd, _buffer.data(), remainingBytes);

        if (nbytes == -1) {
            if (errno == EAGAIN) {
                continue;
            }

            _error = Error(ErrorType::WRITE_PAGE, errno);
            _buffer.resize(0);
            return;
        }

        remainingBytes -= nbytes;
    }

    _buffer.resize(0);
}
