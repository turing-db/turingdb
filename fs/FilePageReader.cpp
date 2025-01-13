#include "FilePageReader.h"

#include <fcntl.h>

using namespace fs;

Result<FilePageReader> FilePageReader::open(const Path& path) {
    const int access = O_RDONLY | O_DIRECT;
    const int permissions = S_IRUSR | S_IWUSR;

    const int fd = ::open(path.c_str(), access, permissions);

    if (fd == -1) {
        return Error::result(ErrorType::OPEN_FILE, errno);
    }

    return FilePageReader {fd};
}

Result<FilePageReader> FilePageReader::openNoDirect(const Path& path) {
    const int access = O_RDONLY;
    const int permissions = S_IRUSR | S_IWUSR;

    const int fd = ::open(path.c_str(), access, permissions);

    if (fd == -1) {
        return Error::result(ErrorType::OPEN_FILE, errno);
    }

    return FilePageReader {fd};
}

Result<void> FilePageReader::nextPage() {
    size_t bytesRead = 0;

    while (bytesRead != PAGE_SIZE) {
        ssize_t nbytes = ::read(_fd, _buffer.data(), PAGE_SIZE);

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

        bytesRead += nbytes;
    }

    _buffer.resize(bytesRead);

    return {};
}
