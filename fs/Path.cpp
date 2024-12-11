#include "Path.h"

#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

using namespace fs;

Path::Path(std::string path)
    : _path(std::move(path))
{
    if (!_path.empty() && _path.back() == '/') {
        _path.pop_back();
    }
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

FileResult<std::vector<Path>> Path::listDir() const {
    auto info = getFileInfo();

    if (!info) {
        return info.get_unexpected();
    }

    if (info->_type != FileType::Directory) {
        return FileError::result(_path.c_str(), "Not a directory");
    }

    DIR* d = ::opendir(_path.c_str());

    if (!d) {
        return FileError::result(_path.c_str(), "Could not open directory");
    }

    std::vector<Path> paths;
    while (const auto* dir = ::readdir(d)) {
        const size_t len = strlen(dir->d_name);
        if (dir->d_name[0] == '.') {
            if (len == 1) {
                continue;
            }

            if (len == 2 && dir->d_name[1] == '.') {
                continue;
            }
        }

        paths.push_back(Path(dir->d_name));
    }

    if (::closedir(d) == -1) {
        return FileError::result(_path.c_str(), "Could not close directory");
    }

    return std::move(paths);
}

std::string_view Path::filename() const {
    const auto pos = _path.find_last_of('/');

    if (pos == std::string::npos) {
        return _path;
    }

    return std::string_view {_path}.substr(pos + 1);
}

std::string_view Path::basename() const {
    const std::string_view fname = filename();
    const auto pos = _path.find_last_of('.');

    if (pos == std::string::npos) {
        return fname;
    }

    return std::string_view{_path}.substr(0, pos);
}

std::string_view Path::extension() const {
    const auto pos = _path.find_last_of('.');

    if (pos == std::string::npos) {
        return "";
    }

    return std::string_view{_path}.substr(pos);
}

FileResult<void> Path::mkdir() {
    if (exists()) {
        return FileError::result(_path.c_str(), "Cannot mkdir, already exists");
    }

    if (::mkdir(_path.c_str(), 0700) == -1) {
        return FileError::result(_path.c_str(), "Cannot mkdir: {}", strerror(errno));
    }

    return {};
}
