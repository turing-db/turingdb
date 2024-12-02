#pragma once

#include <cstdint>

namespace fs {

enum class FileType : uint8_t {
    File = 0,
    Directory,
    Unknown,
};

enum class AccessRights : uint8_t {
    None = 0,
    Read = 1,
    Write = 2,
};

struct FileInfo {
    uint64_t _size {};
    FileType _type {};
    AccessRights _access {};

    [[nodiscard]] bool readable() const {
        return ((uint8_t)_access & (uint8_t)AccessRights::Read) != 0;
    }

    [[nodiscard]] bool writable() const {
        return ((uint8_t)_access & (uint8_t)AccessRights::Write) != 0;
    }
};

}
