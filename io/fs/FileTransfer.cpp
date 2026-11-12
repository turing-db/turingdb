#include "FileTransfer.h"

#include <errno.h>
#include <unistd.h>

#include <algorithm>

using namespace fs;

namespace {

// macOS rejects a read or write of more than INT_MAX bytes with EINVAL
constexpr size_t MAX_TRANSFER_SIZE = size_t {1} << 30;

bool isRetryable(int error) {
    return error == EINTR || error == EAGAIN;
}

}

int fs::writeAll(int fd, const uint8_t* data, size_t size) {
    size_t bytesWritten = 0;

    while (bytesWritten < size) {
        const size_t chunkSize = std::min(size - bytesWritten, MAX_TRANSFER_SIZE);
        const ssize_t nbytes = ::write(fd, data + bytesWritten, chunkSize);

        if (nbytes >= 0) {
            bytesWritten += static_cast<size_t>(nbytes);
        } else if (!isRetryable(errno)) {
            return errno;
        }
    }

    return 0;
}

int fs::readAll(int fd, uint8_t* buffer, size_t size, size_t& bytesRead) {
    bytesRead = 0;

    while (bytesRead < size) {
        const size_t chunkSize = std::min(size - bytesRead, MAX_TRANSFER_SIZE);
        const ssize_t nbytes = ::read(fd, buffer + bytesRead, chunkSize);

        if (nbytes > 0) {
            bytesRead += static_cast<size_t>(nbytes);
        } else if (nbytes == 0) {
            return 0;
        } else if (!isRetryable(errno)) {
            return errno;
        }
    }

    return 0;
}
