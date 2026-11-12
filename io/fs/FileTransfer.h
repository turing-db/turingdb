#pragma once

#include <stddef.h>
#include <stdint.h>

namespace fs {

// Both return 0, or the errno of the system call that failed. readAll stops early only at end of file.
[[nodiscard]] int writeAll(int fd, const uint8_t* data, size_t size);
[[nodiscard]] int readAll(int fd, uint8_t* buffer, size_t size, size_t& bytesRead);

}
