#include "FileUtils.h"

#include <unistd.h>
#include <fcntl.h>

bool files::exists(const files::Path& path) {
    try {
        return std::filesystem::exists(path);
    } catch (const std::filesystem::filesystem_error& e) {
        return false;
    }
}

bool files::createDirectory(const files::Path& path) {
    std::error_code createDirError;
    std::filesystem::create_directory(path, createDirError);
    return !(bool)createDirError;
}

bool files::removeDirectory(const files::Path& path) {
    std::error_code removeDirError;
    std::filesystem::remove_all(path, removeDirError);
    return !(bool)removeDirError;
}

bool files::removeFile(const files::Path& path) {
    std::error_code removeError;
    std::filesystem::remove(path, removeError);
    return !(bool)removeError;
}

bool files::copy(const files::Path& from, const files::Path& to) {
    try {
        std::filesystem::copy(from, to);
        return true;
    } catch (const std::filesystem::filesystem_error& e) {
        return false;
    }
}

bool files::isDirectory(const Path& path) {
    try {
        return std::filesystem::is_directory(path);
    } catch (const std::filesystem::filesystem_error& e) {
        return false;
    }
}

files::Path files::cwd() {
    std::error_code error;
    const auto path = std::filesystem::current_path(error);
    if (error) {
        return Path();
    }
    return path;
}

bool files::writeFile(const Path& path, const std::string& content) {
    int fd = open(path.string().c_str(),
                  O_WRONLY | O_TRUNC | O_CREAT,
                  S_IRUSR | S_IWUSR);
    if (fd < 0) {
        return false;
    }

    const size_t contentSize = content.size();
    ssize_t bytesWritten = write(fd, content.c_str(), contentSize);
    if (bytesWritten < 0 || bytesWritten < (ssize_t)contentSize) {
        return false;
    }

    close(fd);
    
    return true;
}

files::Path files::abspath(const Path& relativePath) {
    std::error_code error;
    const auto path = std::filesystem::absolute(relativePath, error);
    if (error) {
        return Path();
    }

    return path;
}
