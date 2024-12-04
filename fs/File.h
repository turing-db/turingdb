#pragma once

#include "FileRegion.h"
#include "Path.h"
#include "IOType.h"

namespace fs {

template <IOType IO>
class File {
public:
    static constexpr IOType io = IO;

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

    [[nodiscard]] static FileResult<File<IO>> open(Path&& path);
    [[nodiscard]] FileResult<FileRegion<IO>> map();
    FileResult<void> close();

    const Path& getPath() const { return _path; }
    const FileInfo& getInfo() const { return _info; }
    int getDescriptor() const { return _fd; }

private:
    Path _path;
    FileInfo _info {};
    int _fd {-1};
};

using IFile = File<IOType::R>;
using IOFile = File<IOType::RW>;

template <typename T>
concept ReadableFile = std::same_as<T, IFile> || std::same_as<T, IOFile>;

template <typename T>
concept WritableFile = std::same_as<T, IOFile>;

}
