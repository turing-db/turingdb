#pragma once

#include <string_view>
#include <unordered_map>

namespace {
using MapType = std::unordered_map<std::string_view, size_t>;
}

namespace db {

class EmbeddingsSpec {
public:
    using Iterator = MapType::iterator;
    using ConstIterator = MapType::const_iterator;

    Iterator find(std::string_view propName);
    ConstIterator find(std::string_view propName) const;

    ConstIterator begin() const { return _specMap.begin(); };
    ConstIterator end() const { return _specMap.end(); };

    void emplace(std::string_view name, size_t dim) { _specMap.emplace(name, dim); }

private:
    MapType _specMap;
};


}
