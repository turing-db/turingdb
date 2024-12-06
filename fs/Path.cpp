#include "Path.h"

#include <sys/stat.h>
#include <unistd.h>

namespace fs {

Path::Path(std::string path)
    : _path(std::move(path))
{
}

FileResult<FileInfo> Path::getFileInfo() const {
    struct ::stat s {};
    if (::stat(_path.c_str(), &s) != 0) {
        return FileError::result(_path.c_str(), "Does not exist");
    }

    uint8_t access {};

    if (::access(_path.c_str(), R_OK) == 0) {
        access |= (uint8_t)AccessRights::Read;
    }

    if (::access(_path.c_str(), W_OK) == 0) {
        access |= (uint8_t)AccessRights::Write;
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

    return FileInfo {
        ._size = size,
        ._type = type,
        ._access = static_cast<AccessRights>(access),
    };
}

}
