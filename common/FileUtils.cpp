#include "FileUtils.h"

#include <unistd.h>
#include <fcntl.h>
#include <fstream>
#include <sys/stat.h>
#include <regex>
#include <string.h>

#include "StringUtils.h"

bool FileUtils::exists(const FileUtils::Path& path) {
    std::error_code error;
    const bool res = std::filesystem::exists(path, error);
    return error ? false : res;
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
    std::error_code error;
    const bool res = std::filesystem::is_directory(path, error);
    return error ? false : res;
}

FileUtils::Path FileUtils::cwd() {
    std::error_code error;
    const auto path = std::filesystem::current_path(error);
    return error ? Path() : path;
}

bool FileUtils::writeFile(const Path& path, const std::string& content) {
    const int fd = open(path.string().c_str(),
                  O_WRONLY | O_TRUNC | O_CREAT,
                  S_IRUSR | S_IWUSR);
    if (fd < 0) {
        return false;
    }

    const size_t contentSize = content.size();
    const ssize_t bytesWritten = write(fd, content.c_str(), contentSize);
    if (bytesWritten < 0 || bytesWritten < (ssize_t)contentSize) {
        close (fd);
        return false;
    }

    close(fd);

    return true;
}

FileUtils::Path FileUtils::abspath(const Path& relativePath) {
    std::error_code error;
    const auto path = std::filesystem::absolute(relativePath, error);
    return error ? Path() : path;
}

bool FileUtils::writeBinary(const Path& path, const char* data, size_t size) {
    const int fd = open(path.string().c_str(),
                  O_WRONLY | O_TRUNC | O_CREAT,
                  S_IRUSR | S_IWUSR);
    if (fd < 0) {
        return false;
    }

    const ssize_t bytesWritten = write(fd, data, size);
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

int FileUtils::openForRead(const Path& path) {
    return open(path.string().c_str(), O_RDONLY);
}

int FileUtils::openForWrite(const Path& path) {
    return open(path.string().c_str(),
                O_WRONLY | O_TRUNC | O_CREAT,
                S_IRUSR | S_IWUSR);
}

bool FileUtils::isFile(const Path& path) {
    std::error_code error;
    const bool res = std::filesystem::is_regular_file(path, error);
    return error ? false : res;
}

bool FileUtils::readContent(const Path& path, std::string& data) {
    if (!isFile(path)) {
        return false;
    }

    std::ifstream file(path, std::ios::in);

    if (!file.is_open())
        return false;

    // Read contents
    data = {std::istreambuf_iterator<char>(file),
            std::istreambuf_iterator<char>()};

    return true;
}

bool FileUtils::listFiles(const Path& dir, std::vector<Path>& paths) {
    try {
        if (!FileUtils::exists(dir)) {
            return false;
        }

        if (!FileUtils::isDirectory(dir)) {
            return false;
        }

        for (const auto& entry : std::filesystem::directory_iterator(dir)) {
            paths.push_back(entry);
        }
    } catch (std::filesystem::filesystem_error& e) {
        return false;
    }

    return true;
}

uint64_t FileUtils::fileSize(const Path& path) {
    return std::filesystem::file_size(path);
}

void FileUtils::getExtension(const Path& path, std::string& result) {
    result = path.extension();
}

void FileUtils::getNameWithoutExtension(const Path& path, std::string& result) {
    result = path.stem();
}

bool FileUtils::makeExecutable(const Path& path) {
    std::error_code error;

    const auto execPerms = std::filesystem::perms::owner_exec
                         | std::filesystem::perms::group_exec
                         | std::filesystem::perms::others_exec;

    std::filesystem::permissions(path, execPerms, std::filesystem::perm_options::add, error);
    return !error;
}

bool FileUtils::isAbsolute(const Path& path) {
    try {
        return path.is_absolute();
    } catch (std::filesystem::filesystem_error& e) {
        return false;
    }
}

bool FileUtils::isOpenedDescriptor(int fd) {
    struct stat statBuf;
    const int res = fstat(fd, &statBuf);
    return (res != EBADF && res != EIO);
}

bool FileUtils::isExecutable(const Path& path) {
    if (!isFile(path)) {
        return false;
    }

    std::error_code error;
    const auto perms = std::filesystem::status(path, error).permissions();

    return (perms & std::filesystem::perms::owner_exec) != std::filesystem::perms::none ||
           (perms & std::filesystem::perms::group_exec) != std::filesystem::perms::none ||
           (perms & std::filesystem::perms::others_exec) != std::filesystem::perms::none;
}

void FileUtils::findExecutableInPath(std::string_view exe, std::string& result) {
    result.clear();

    if (exe.empty()) {
        return;
    }

    const char* pathEnv = std::getenv("PATH");
    if (!pathEnv) {
        return;
    }

    const std::string_view pathView(pathEnv);
    std::vector<std::string_view> pathElements;
    StringUtils::splitString(pathView, ':', pathElements);

    for (const auto& dir : pathElements) {
        if (dir.empty() || dir[0] != '/') {
            continue;
        }

        const Path execPath = Path(dir)/exe;
        if (isExecutable(execPath)) {
            result = execPath.string();
            return;
        }
    }
}

void FileUtils::expandPath(const std::string& path, std::string& result) {
    const std::regex envVarRegex("\\$\\{([^}]+)\\}|\\$([a-zA-Z0-9_]+)");

    std::smatch match;
    auto searchStart = path.cbegin();
    result = path;
    while (std::regex_search(searchStart, path.cend(), match, envVarRegex)) {
        // Get variable name, either in group 1 or in group 2
        const auto& varName = match[1].matched ? match[1].str() : match[2].str();

        // Get environment variable
        const char* envValue = getenv(varName.c_str());
        if (envValue) {
            // Replace the variable with its value
            const auto startPos = match.position(0) + (searchStart - path.cbegin());
            result.replace(startPos, match.length(0), envValue);

            // Update the search start position
            searchStart = path.cbegin() + startPos + strlen(envValue);
        } else {
            // Variable not found, leave it unchanged
            searchStart = path.cbegin() + match.position(0) + match.length(0);
        }
    }
}
