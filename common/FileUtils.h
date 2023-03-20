#ifndef _BIO_COMMON_FILE_UTILS_
#define _BIO_COMMON_FILE_UTILS_

#include <filesystem>

namespace files {

using Path = std::filesystem::path;

bool exists(const Path& path);
bool createDirectory(const Path& path);
bool removeDirectory(const Path& path);
bool copy(const Path& from, const Path& to);
bool isDirectory(const Path& path);
bool removeFile(const Path& path);

}

#endif
