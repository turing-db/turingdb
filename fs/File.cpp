#include "File.h"

#include <fcntl.h>
#include <sys/mman.h>

#include "BioAssert.h"

using namespace fs;

File::~File() {
    if (_fd != -1) {
        ::close(_fd);
    }
}

FileResult<File> File::open(Path&& path) {
    const int access = O_RDWR | O_CREAT | O_APPEND;
    const int permissions = S_IRUSR | S_IWUSR;

    const int fd = ::open(path.c_str(), access, permissions);

    if (fd == -1) {
        return FileError::result(path.c_str(),
                                 "Could not open file",
                                 ::strerror(errno));
    }

    const auto info = path.getFileInfo();

    if (!info) {
        return info.get_unexpected();
    }

    File file;
    file._fd = fd;
    file._path = std::move(path);
    file._info = info.value();

    return std::move(file);
}

FileResult<FileRegion> File::map(size_t size, size_t offset) {
    const int prot = PROT_READ | PROT_WRITE;
    static const size_t pageSize = sysconf(_SC_PAGE_SIZE);
    const size_t alignment = (offset / pageSize) * pageSize;
    const size_t alignmentOffset = (offset % pageSize);

    char* map = (char*)::mmap(nullptr, size + alignmentOffset, prot, MAP_SHARED, _fd, alignment);

    if (map == MAP_FAILED) {
        ::close(_fd);
        return FileError::result(_path.c_str(),
                                 "Could not map file",
                                 ::strerror(errno));
    }

    return FileRegion {map, size, alignmentOffset};
}

FileResult<void> File::reopen() {
    if (auto res = close(); !res) {
        return res;
    }

    const int access = O_RDWR | O_APPEND;
    const int permissions = S_IRUSR | S_IWUSR;

    _fd = ::open(_path.c_str(), access, permissions);

    if (_fd == -1) {
        return FileError::result(_path.c_str(),
                                 "Could not re-open file",
                                 ::strerror(errno));
    }

    return refreshInfo();
}

FileResult<void> File::read(void* buf, size_t size) const {
    bioassert(size <= std::numeric_limits<ssize_t>::max());

    while (size != 0) {
        const ssize_t nbytes = ::read(_fd, buf, size);

        if (nbytes < 0) {
            return FileError::result(_path.c_str(),
                                     "Could not read file",
                                     ::strerror(errno));
        }

        if (nbytes == 0) {
            // EOF
            break;
        }

        size -= nbytes;
    }

    return {};
}

FileResult<void> File::write(void* data, size_t size) {
    bioassert(size <= std::numeric_limits<ssize_t>::max());

    while (size != 0) {
        const ssize_t nbytes = ::write(_fd, data, size);

        if (nbytes < 0) {
            return FileError::result(_path.c_str(),
                                     "Could not write file",
                                     ::strerror(errno));
        }

        size -= nbytes;
    }

    return refreshInfo();
}

FileResult<void> File::clearContent() {
    if (auto res = this->close(); !res) {
        return res;
    }

    const int access = O_RDWR | O_CREAT | O_TRUNC | O_APPEND;
    const int permissions = S_IRUSR | S_IWUSR;

    const int fd = ::open(_path.c_str(), access, permissions);

    if (fd == -1) {
        return FileError::result(_path.c_str(),
                                 "Could clear file content",
                                 ::strerror(errno));
    }

    _fd = fd;

    return refreshInfo();
}

FileResult<void> File::refreshInfo() {
    const auto info = _path.getFileInfo();

    if (!info) {
        return info.get_unexpected();
    }

    _info = info.value();

    return {};
}


FileResult<void> File::close() {
    if (::close(_fd) != 0) {
        return FileError::result(_path.c_str(),
                                 "Could not close file",
                                 ::strerror(errno));
    }

    _fd = -1;

    return {};
}

