#pragma once

#include <stddef.h>
#include <string>

namespace db {

class DBIndex {
public:
    using ID = size_t;

    explicit DBIndex(ID objectID)
        : _objectID(objectID)
    {
    }

    ID getObjectID() const { return _objectID; }

    bool operator==(const DBIndex& other) const {
        return _objectID == other._objectID;
    }

    bool operator!=(const DBIndex& other) const {
        return !(*this == other);
    }

    bool operator<(const DBIndex& other) const {
        return _objectID < other._objectID;
    }

    operator size_t() const {
        return _objectID;
    }

    std::string toString() const;

private:
    ID _objectID;
};

}

namespace std {
template <>
struct hash<db::DBIndex> {
    size_t operator()(db::DBIndex id) const {
        return (size_t)id;
    }
};
}
