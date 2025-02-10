#include "Path.h"

#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <filesystem>

using namespace fs;

Path::Path(const std::string& path)
    : _path(path)
{
    if (!_path.empty() && _path.back() == '/') {
        _path.pop_back();
    }
}

Path::Path(std::string&& path)
    : _path(std::move(path))
{
    if (!_path.empty() && _path.back() == '/') {
        _path.pop_back();
    }
}

Result<FileInfo> Path::getFileInfo() const {
    struct ::stat s {};
    if (::stat(_path.c_str(), &s) != 0) {
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

    return FileInfo {
        ._size = size,
        ._type = type,
        ._access = static_cast<AccessRights>(access),
    };
}

Result<std::vector<Path>> Path::listDir() const {
    auto info = getFileInfo();

    if (!info) {
        return info.get_unexpected();
    }

    if (info->_type != FileType::Directory) {
        return Error::result(ErrorType::NOT_DIRECTORY);
    }

    DIR* d = ::opendir(_path.c_str());

    if (!d) {
        return Error::result(ErrorType::OPEN_DIRECTORY, errno);
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
        return Error::result(ErrorType::CLOSE_DIRECTORY);
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

    return std::string_view {_path}.substr(0, pos);
}

std::string_view Path::extension() const {
    const auto pos = _path.find_last_of('.');

    if (pos == std::string::npos) {
        return "";
    }

    return std::string_view {_path}.substr(pos);
}

Result<void> Path::mkdir() const {
    if (exists()) {
        return Error::result(ErrorType::ALREADY_EXISTS);
    }

    if (::mkdir(_path.c_str(), 0700) < 0) {
        return Error::result(ErrorType::CANNOT_MKDIR, errno);
    }

    return {};
}

Result<void> Path::rm() const {
    std::error_code err {};
    std::filesystem::remove_all(_path, err);
    if (err) {
        return Error::result(ErrorType::CANNOT_REMOVE, err.value());
    }

    return {};
}

