#pragma once

#include <string>

#include "FileInfo.h"
#include "FileResult.h"

namespace fs {

class Path {
public:
    Path() = default;
    explicit Path(std::string path);

    ~Path() = default;

    Path(const Path& path) = default;
    Path(Path&& path) noexcept = default;
    Path& operator=(const Path& path) = default;
    Path& operator=(Path&& path) noexcept = default;

    [[nodiscard]] FileResult<FileInfo> getFileInfo() const;
    [[nodiscard]] const std::string& get() const { return _path; }
    [[nodiscard]] const char* c_str() const { return _path.c_str(); }
    [[nodiscard]] Path copy() const { return Path(_path); }

private:
    std::string _path;
};

}
