#pragma once

#include <string>
#include <vector>
#include <compare>

#include "FileInfo.h"
#include "FileResult.h"

namespace fs {

class Path {
public:
    Path() = default;
    explicit Path(const std::string& path);
    explicit Path(std::string&& path);

    ~Path() = default;

    Path(const Path& path) = default;
    Path(Path&& path) noexcept = default;
    Path& operator=(const Path& path) = default;
    Path& operator=(Path&& path) noexcept = default;

    [[nodiscard]] Result<FileInfo> getFileInfo() const;
    [[nodiscard]] const std::string& get() const { return _path; }
    [[nodiscard]] const char* c_str() const { return _path.c_str(); }
    [[nodiscard]] Path copy() const { return Path(_path); }
    [[nodiscard]] bool exists() const { return getFileInfo().has_value(); }
    [[nodiscard]] Result<std::vector<Path>> listDir() const;
    [[nodiscard]] std::string_view filename() const;
    [[nodiscard]] std::string_view basename() const;
    [[nodiscard]] std::string_view extension() const;

    [[nodiscard]] Path operator/(std::string_view rhs) const {
        std::string p = _path + "/";
        p += rhs;
        return Path(std::move(p));
    }

    Path& operator/=(std::string_view rhs) {
        _path += "/";
        _path += rhs;
        return *this;
    }

    Path& operator+=(std::string_view rhs) {
        _path += rhs;
        return *this;
    }

    Path operator+(std::string_view rhs) {
        Path newPath = *this;
        newPath += rhs;
        return newPath;
    }

    std::strong_ordering operator<=>(const Path& rhs) const {
        return _path <=> rhs.get();
    }

    bool operator==(const Path& rhs) const {
        return _path == rhs.get();
    }

    Result<void> mkdir() const;
    Result<void> rm() const;

private:
    std::string _path;
};

}
