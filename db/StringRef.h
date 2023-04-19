// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <utility>

#include "SharedString.h"

namespace db {

class StringRef {
public:
    using iterator = const char*;

    struct Comparator {
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

    bool operator==(const StringRef& other) const {
        return _sharedStr == other._sharedStr; 
    }

    bool operator<(const StringRef& other) const {
        return Comparator()(*this, other);
    }

    bool empty() const { return _sharedStr == nullptr; }

    std::size_t size() const;

    iterator begin() const;
    iterator end() const;

    const SharedString* getSharedString() const { return _sharedStr; }

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
