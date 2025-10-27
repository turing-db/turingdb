#pragma once

#include <limits>
#include <string>
#include <utility>

namespace db {

class ColumnNameManager;

class ColumnNameSharedString {
public:
    friend ColumnNameManager;

    const std::string& getString() const { return _str; }

private:
    std::string _str;

    ColumnNameSharedString(const std::string& str)
        : _str(str)
    {}

    ColumnNameSharedString(std::string&& str)
        : _str(std::move(str))
    {}

    ~ColumnNameSharedString() = default;
};

class ColumnName {
public:
    using Tag = size_t;

    ColumnName(Tag tag)
        : _tag(tag)
    {
    }

    ColumnName(ColumnNameSharedString* sharedStr)
        : _sharedStr(sharedStr)
    {
    }

    ~ColumnName() = default;

    bool operator==(const ColumnName& other) const {
        if (_sharedStr) {
            return _sharedStr == other._sharedStr;
        } else {
            return _tag == other._tag;
        }
    }

    bool hasString() const { return _sharedStr != nullptr; }
    bool hasTag() const { return _tag != INVALID_TAG; }

    Tag getTag() const { return _tag; }

    ColumnNameSharedString* getString() const {
        return _sharedStr; 
    }

    std::string toStdString() const {
        if (_sharedStr) {
            return _sharedStr->getString();
        } else {
            return "$"+std::to_string(_tag);
        }
    }

private:
    static constexpr Tag INVALID_TAG =
        std::numeric_limits<Tag>::max();

    Tag _tag {INVALID_TAG};
    ColumnNameSharedString* _sharedStr {nullptr};
};

struct ColumnNameHash {
    std::size_t operator()(const ColumnName& name) const {
        if (name.hasString()) {
            return std::hash<std::string>()(name.getString()->getString());
        } else {
            return std::hash<std::size_t>()(name.getTag());
        }
    }
};

}
