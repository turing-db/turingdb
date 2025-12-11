#include "File.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "BioAssert.h"

using namespace fs;

File::~File() {
    if (_fd != -1) {
        ::close(_fd);
    }
}

Result<File> File::createAndOpen(const Path& path) {
    const int access = O_RDWR | O_CREAT | O_APPEND;
    const int permissions = S_IRUSR | S_IWUSR;

    const int fd = ::open(path.c_str(), access, permissions);

    if (fd < 0) {
        return Error::result(ErrorType::OPEN_FILE, errno);
    }

    const auto info = path.getFileInfo();

    if (!info) {
        return BadResult(info.error());
    }

    File file;
    file._fd = fd;
    file._info = info.value();

    return std::move(file);
}

Result<File> File::open(const Path& path) {
    const int access = O_RDWR | O_APPEND;
    const int permissions = S_IRUSR | S_IWUSR;

    const int fd = ::open(path.c_str(), access, permissions);

    if (fd < 0) {
        return Error::result(ErrorType::OPEN_FILE, errno);
    }

    const auto info = path.getFileInfo();

    if (!info) {
        return BadResult(info.error());
    }

    File file;
    file._fd = fd;
    file._info = info.value();

    return std::move(file);
}

Result<FileRegion> File::map(size_t size, size_t offset) {
    const int prot = PROT_READ | PROT_WRITE;
    static const size_t pageSize = sysconf(_SC_PAGE_SIZE);
    const size_t alignment = (offset / pageSize) * pageSize;
    const size_t alignmentOffset = (offset % pageSize);

    void* map = (char*)::mmap(nullptr, size + alignmentOffset, prot, MAP_SHARED, _fd, alignment);

    if (map == MAP_FAILED) {
        const int err = errno;
        ::close(_fd);
        return Error::result(ErrorType::MAP, err);
    }

    return FileRegion {static_cast<char*>(map), size, alignmentOffset};
}

Result<void> File::seek(size_t offset) {
    if (::lseek(_fd, offset, SEEK_SET) < 0) {
        return Error::result(ErrorType::SEEK_FILE, errno);
    }

    return {};
}

Result<void> File::read(void* buf, size_t size) const {
    ssize_t remainingBytes = size;
    while (remainingBytes > 0) {
        const ssize_t nbytes = ::read(_fd, buf, remainingBytes);

        if (nbytes < 0) {
            return Error::result(ErrorType::READ_FILE, errno);
        }

        if (nbytes == 0) {
            // EOF
            break;
        }

        remainingBytes -= nbytes;
    }

    return {};
}

Result<void> File::write(void* data, size_t size) {
    while (size != 0) {
        const ssize_t nbytes = ::write(_fd, data, size);

        if (nbytes < 0) {
            return Error::result(ErrorType::WRITE_FILE, errno);
        }

        size -= nbytes;
    }

    return refreshInfo();
}

Result<void> File::clearContent() {
    int res = ::ftruncate(_fd, 0);

    if (res < 0) {
        return Error::result(ErrorType::CLEAR_FILE, errno);
    }

    return refreshInfo();
}

Result<void> File::refreshInfo() {
    struct ::stat s {};
    if (::fstat(_fd, &s) != 0) {
        return Error::result(ErrorType::NOT_EXISTS, errno);
    }

    const uid_t euid = geteuid();
    const gid_t egid = getegid();

    uint8_t access {};

    if (euid == s.st_uid) {
        // If file belongs to user
        if ((s.st_mode & S_IRUSR) != 0) {
            access |= (uint8_t)AccessRights::Read;
        }

        if ((s.st_mode & S_IWUSR) != 0) {
            access |= (uint8_t)AccessRights::Write;
        }
    } else if (egid == s.st_gid) {
        // If file belongs to group
        if ((s.st_mode & S_IRGRP) != 0) {
            access |= (uint8_t)AccessRights::Read;
        }

        if ((s.st_mode & S_IWGRP) != 0) {
            access |= (uint8_t)AccessRights::Write;
        }
    } else {
        // If file belongs to others
        if ((s.st_mode & S_IROTH) != 0) {
            access |= (uint8_t)AccessRights::Read;
        }

        if ((s.st_mode & S_IWOTH) != 0) {
            access |= (uint8_t)AccessRights::Write;
        }
    }

    FileType type {};
    if (S_ISREG(s.st_mode)) {
        type = FileType::File;
    } else if (S_ISDIR(s.st_mode)) {
        type = FileType::Directory;
    } else {
        type = FileType::Unknown;
    }

    const size_t size = type == FileType::File
                          ? s.st_size
                          : 0;

    _info = FileInfo {
        ._size = size,
        ._type = type,
        ._access = static_cast<AccessRights>(access),
    };

    return {};
}


Result<void> File::close() {
    if (::close(_fd) != 0) {
        return Error::result(ErrorType::CLOSE_FILE, errno);
    }

    _fd = -1;

    return {};
}

