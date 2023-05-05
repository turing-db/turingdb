#include "FileUtils.h"

#include <unistd.h>
#include <fcntl.h>

bool FileUtils::exists(const FileUtils::Path& path) {
    try {
        return std::filesystem::exists(path);
    } catch (const std::filesystem::filesystem_error& e) {
        return false;
    }
}

bool FileUtils::createDirectory(const FileUtils::Path& path) {
    std::error_code createDirError;
    std::filesystem::create_directories(path, createDirError);
    return !(bool)createDirError;
}

bool FileUtils::removeDirectory(const FileUtils::Path& path) {
    std::error_code removeDirError;
    std::filesystem::remove_all(path, removeDirError);
    return !(bool)removeDirError;
}

bool FileUtils::removeFile(const FileUtils::Path& path) {
    std::error_code removeError;
    std::filesystem::remove(path, removeError);
    return !(bool)removeError;
}

bool FileUtils::copy(const FileUtils::Path& from, const FileUtils::Path& to) {
    try {
        std::filesystem::copy(from, to);
        return true;
    } catch (const std::filesystem::filesystem_error& e) {
        return false;
    }
}

bool FileUtils::isDirectory(const Path& path) {
    try {
        return std::filesystem::is_directory(path);
    } catch (const std::filesystem::filesystem_error& e) {
        return false;
    }
}

FileUtils::Path FileUtils::cwd() {
    std::error_code error;
    const auto path = std::filesystem::current_path(error);
    if (error) {
        return Path();
    }
    return path;
}

bool FileUtils::writeFile(const Path& path, const std::string& content) {
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

FileUtils::Path FileUtils::abspath(const Path& relativePath) {
    std::error_code error;
    const auto path = std::filesystem::absolute(relativePath, error);
    if (error) {
        return Path();
    }

    return path;
}

bool FileUtils::writeBinary(const Path& path, const char* data, size_t size) {
    int fd = open(path.string().c_str(),
                  O_WRONLY | O_TRUNC | O_CREAT,
                  S_IRUSR | S_IWUSR);
    if (fd < 0) {
        return false;
    }

    ssize_t bytesWritten = write(fd, data, size);
    if (bytesWritten < 0 || bytesWritten < (ssize_t)size) {
        return false;
    }

    close(fd);
    
    return true;
}

FileUtils::Path FileUtils::getFilename(const Path& path) {
    if (path.empty()) {
        return Path();
    }

    return path.filename();
}
