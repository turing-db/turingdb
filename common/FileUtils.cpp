#include "FileUtils.h"

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
