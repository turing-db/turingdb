#include "File.h"

#include <fcntl.h>
#include <sys/mman.h>

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

    if (!info) {
        return info.get_unexpected();
    }

    if (info->_type != FileType::File) {
        return FileError::result(path.get(), "Not a file");
    }

    if constexpr (WritableFile<File<IO>>) {
        if (!info->readable()) {
            return FileError::result(path.get(), "Read access denied");
        }
    }

    if constexpr (ReadableFile<File<IO>>) {
        if (!info->writable()) {
            return FileError::result(path.get(), "Write access denied");
        }
    }

    int access = 0;

    if constexpr (IO == IOType::R) {
        access = O_RDONLY;
    } else if constexpr (IO == IOType::RW) {
        access = O_RDWR;
    } else {
        ([]<bool flag = false> { static_assert(flag); })();
    }

    const int fd = ::open(path.get().data(), access);

    if (fd == -1) {
        return FileError::result(path.get(),
                                 "Could not open file: {}",
                                 ::strerror(errno));
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
