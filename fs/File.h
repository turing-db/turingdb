#pragma once

#include "FileRegion.h"
#include "Path.h"

namespace fs {

class File {
public:
    File() = default;
    ~File();

    File(File&& other) noexcept
        : _path(std::move(other._path)),
          _info(other._info),
          _fd(other._fd)
    {
        other._fd = -1;
    }

    File& operator=(File&& other) noexcept {
        if (&other == this) {
            return *this;
        }

        _path = std::move(other._path);
        _info = other._info;
        _fd = -1;

        return *this;
    }

    File(const File&) = delete;
    File& operator=(const File&) = delete;

    [[nodiscard]] static FileResult<File> open(Path&& path);
    [[nodiscard]] FileResult<FileRegion> map(size_t size, size_t offset = 0);
    FileResult<void> reopen();
    FileResult<void> read(void* buf, size_t size) const;
    FileResult<void> write(void* data, size_t size);
    FileResult<void> clearContent();
    FileResult<void> refreshInfo();
    FileResult<void> close();

    const Path& getPath() const { return _path; }
    const FileInfo& getInfo() const { return _info; }
    int getDescriptor() const { return _fd; }

private:
    Path _path;
    FileInfo _info {};
    int _fd {-1};
};

}
