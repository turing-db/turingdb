#include "FilePageReader.h"

#include <fcntl.h>
#include <unistd.h>

#include "FileTransfer.h"

using namespace fs;

Result<FilePageReader> FilePageReader::open(const Path& path, size_t pageSize) {
#ifdef __APPLE__
    const int access = O_RDONLY;
#else
    const int access = O_RDONLY | O_DIRECT;
#endif
    const int permissions = S_IRUSR | S_IWUSR;

    const int fd = ::open(path.c_str(), access, permissions);

    if (fd < 0) {
        return Error::result(ErrorType::OPEN_FILE, errno);
    }

#ifdef __APPLE__
    // Disable caching on macOS (equivalent to O_DIRECT)
    fcntl(fd, F_NOCACHE, 1);
#endif

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
    const size_t capacity = _buffer.capacity();
    size_t bytesRead = 0;

    if (const int error = readAll(_fd, _buffer.data(), capacity, bytesRead); error != 0) {
        return Error::result(ErrorType::READ_PAGE, error);
    }

    if (bytesRead < capacity) {
        _reachedEnd = true;
    }

    _buffer.resize(bytesRead);

    return {};
}
