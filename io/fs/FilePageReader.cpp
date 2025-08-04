#include "FilePageReader.h"

#include <fcntl.h>
#include <unistd.h>

using namespace fs;

Result<FilePageReader> FilePageReader::open(const Path& path, size_t pageSize) {
    const int access = O_RDONLY | O_DIRECT;
    const int permissions = S_IRUSR | S_IWUSR;

    const int fd = ::open(path.c_str(), access, permissions);

    if (fd < 0) {
        return Error::result(ErrorType::OPEN_FILE, errno);
    }

    return FilePageReader {fd, pageSize};
}

Result<FilePageReader> FilePageReader::openNoDirect(const Path& path, size_t pageSize) {
    const int access = O_RDONLY;
    const int permissions = S_IRUSR | S_IWUSR;

    const int fd = ::open(path.c_str(), access, permissions);

    if (fd < 0) {
        return Error::result(ErrorType::OPEN_FILE, errno);
    }

    return FilePageReader {fd, pageSize};
}

Result<void> FilePageReader::nextPage() {
    ssize_t remainingBytes = _buffer.capacity();

    while (remainingBytes > 0) {
        const ssize_t nbytes = ::read(_fd, _buffer.data(), remainingBytes);

        if (nbytes == -1) {
            if (errno == EAGAIN) {
                continue;
            }

            return Error::result(ErrorType::READ_PAGE, errno);
        }

        if (nbytes == 0) {
            _reachedEnd = true;
            break; // Finished reading file
        }

        remainingBytes -= nbytes;
    }

    bioassert(remainingBytes >= 0);
    _buffer.resize(_buffer.capacity() - remainingBytes);

    return {};
}
