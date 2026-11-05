#pragma once

#include <string_view>
#include <unordered_map>
#include <vector>

#include "metadata/PropertyType.h"

namespace db {

// The properties an import reads as a type its file format does not spell: a datetime is a
// JSON string, a duration an integer count of microseconds, a point a JSON object. The query
// names them, so the import does not guess from what a value looks like.
class TypedPropertySpec {
public:
    // A name the query gave a second type. The first one stands.
    struct Clash {
        std::string_view _name;
        ValueType _first {ValueType::Invalid};
        ValueType _second {ValueType::Invalid};
    };

    using MapType = std::unordered_map<std::string_view, ValueType>;
    using ConstIterator = MapType::const_iterator;

    // ValueType::Invalid where the query names no type for @param propertyName
    ValueType getType(std::string_view propertyName) const;

    bool contains(std::string_view propertyName) const;

    void emplace(std::string_view propertyName, ValueType type);

    const std::vector<Clash>& getClashes() const { return _clashes; }

    ConstIterator begin() const { return _types.begin(); }
    ConstIterator end() const { return _types.end(); }

    bool empty() const { return _types.empty(); }

private:
    MapType _types;
    std::vector<Clash> _clashes;
};

}
