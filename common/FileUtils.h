#pragma once

#include <filesystem>
#include <vector>

class FileUtils {
public:
    using Path = std::filesystem::path;

    static bool exists(const Path& path);
    static bool createDirectory(const Path& path);
    static bool removeDirectory(const Path& path);
    static bool copy(const Path& from, const Path& to);
    static bool isDirectory(const Path& path);
    static bool removeFile(const Path& path);
    static Path cwd();
    static Path abspath(const Path& relativePath);
    static Path getFilename(const Path& path);
    static bool writeFile(const Path& path, const std::string& content);
    static bool writeBinary(const Path& path, const char* data, size_t size);
    static int openForRead(const Path& path);
    static int openForWrite(const Path& path);
    static bool readContent(const Path& path, std::string& data);
    static bool listFiles(const Path& dir, std::vector<Path>& paths);
};
