// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <utility>

#include "SharedString.h"

namespace db {

class StringRef {
public:
    using iterator = const char*;

    struct Sorter {
        bool operator()(StringRef lhs, StringRef rhs) const {
            if (!lhs._sharedStr) {
                return true;
            }
            if (!rhs._sharedStr) {
                return false;
            }
            return lhs._sharedStr->getID() < rhs._sharedStr->getID();
        }
    };

    StringRef() = default;
    StringRef(const SharedString* sharedStr);
    ~StringRef() = default;

    friend std::string operator+(StringRef lhs, const std::string& rhs) {
        return lhs._sharedStr ? lhs._sharedStr->getString() + rhs : rhs;
    }

    friend std::string operator+(const std::string& lhs, StringRef rhs) {
        return rhs._sharedStr ? lhs + rhs._sharedStr->getString() : lhs;
    }

    static bool same(const StringRef& lhs, const StringRef& rhs) {
        if (!lhs._sharedStr && !rhs._sharedStr) {
            return true;
        }
        if (!lhs._sharedStr || !rhs._sharedStr) {
            return false;
        }

        return lhs.getID() == rhs.getID()
            && lhs._sharedStr->getString() == rhs._sharedStr->getString();
    }

    bool operator==(const StringRef& other) const {
        return _sharedStr == other._sharedStr;
    }

    bool operator<(const StringRef& other) const {
        return Sorter()(*this, other);
    }

    bool empty() const { return _sharedStr == nullptr; }

    std::size_t size() const;

    iterator begin() const;
    iterator end() const;

    const SharedString* getSharedString() const { return _sharedStr; }

    SharedString::ID getID() const {
        return _sharedStr ? _sharedStr->getID() : 0;
    }

    std::string toStdString() const {
        return _sharedStr ? _sharedStr->getString() : std::string();
    }

private:
    const SharedString* _sharedStr {nullptr};
};

}

namespace std {
template <>
struct hash<db::StringRef> {
    size_t operator()(const db::StringRef& str) const {
        const db::SharedString* sharedStr = str.getSharedString();
        return hash<size_t>()(sharedStr ? sharedStr->getID() : 0);
    }
};
}
