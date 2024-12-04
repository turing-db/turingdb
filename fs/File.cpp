#include "File.h"

#include <fcntl.h>
#include <sys/mman.h>

#include "Panic.h"

using namespace fs;

template <IOType IO>
File<IO>::~File() {
    if (_fd != -1) {
        ::close(_fd);
    }
}

template <IOType IO>
FileResult<File<IO>> File<IO>::open(Path&& path) {
    auto info = path.getFileInfo();

    if constexpr (!WritableFile<File<IO>>) {
        if (!info) {
            return info.get_unexpected();
        }
    }

    int access = 0;
    int permissions = 0;

    if constexpr (IO == IOType::R) {
        access = O_RDONLY;
        permissions = S_IRUSR;
    } else if constexpr (IO == IOType::RW) {
        access = O_WRONLY| O_CREAT | O_TRUNC;
        permissions = S_IRUSR | S_IWUSR;
    } else {
        COMPILE_ERROR("Added an IO Type");
    }
    // R  -> Read
    // RW  -> Write + Create
    // RA  -> Write + Append + Create
    // RW -> Read + Write + Create
    // RC  -> Create (Write + Truncate + Append + Create)

    const int fd = ::open(path.get().data(), access, permissions);

    if (fd == -1) {
        return FileError::result(path.get(),
                                 "Could not open file: {}",
                                 ::strerror(errno));
    }

    if constexpr (WritableFile<File<IO>>) {
        if (!info) {
            info = path.getFileInfo();
        }

        if (!info) {
            return info.get_unexpected();
        }
    }

    File file;
    file._fd = fd;
    file._path = std::move(path);
    file._info = info.value();

    return std::move(file);
}

template <IOType IO>
FileResult<void> File<IO>::close() {
    if (::close(_fd) != 0) {
        return FileError::result(_path.get(),
                                 "Could not close file: {}",
                                 ::strerror(errno));
    }

    _fd = -1;

    return {};
}

template <IOType IO>
FileResult<FileRegion<IO>> File<IO>::map() {
    int prot = 0;
    if constexpr (IO == IOType::R) {
        prot = PROT_READ;
    } else if constexpr (IO == IOType::RW) {
        prot = PROT_READ | PROT_WRITE;
    } else {
        ([]<bool flag = false> { static_assert(flag); })();
    }

    char* map = (char*)mmap(nullptr, _info._size, prot, MAP_SHARED, _fd, 0);

    if (map == MAP_FAILED) {
        ::close(_fd);
        return FileError::result(_path.get(),
                                 "Could not map file: {}",
                                 ::strerror(errno));
    }

    return FileRegion<IO> {map, _info._size};
}

template class fs::File<IOType::R>;
template class fs::File<IOType::RW>;
