#pragma once

#include "FileRegion.h"
#include "Path.h"

namespace fs {

class File {
public:
    File() = default;
    ~File();

    File(File&& other) noexcept
        : _info(other._info),
        _fd(other._fd)
    {
        other._fd = -1;
    }

    File& operator=(File&& other) noexcept {
        if (&other == this) {
            return *this;
        }

        _info = other._info;
        _fd = other._fd;
        other._fd = -1;

        return *this;
    }

    File(const File&) = default;
    File& operator=(const File&) = default;

    [[nodiscard]] static Result<File> createAndOpen(const Path& path);
    [[nodiscard]] static Result<File> open(const Path& path);
    [[nodiscard]] Result<FileRegion> map(size_t size, size_t offset = 0);
    Result<void> seek(size_t offset);
    Result<void> read(void* buf, size_t size) const;
    Result<void> write(void* data, size_t size);
    Result<void> clearContent();
    Result<void> refreshInfo();
    Result<void> close();

    const FileInfo& getInfo() const { return _info; }
    int getDescriptor() const { return _fd; }

private:
    FileInfo _info {};
    int _fd {-1};
};

}
