#pragma once

#include <optional>
#include <string>
#include <unordered_map>

#include "ID.h"

namespace db {

class EdgeTypeMap {
public:
    struct Pair {
        EdgeTypeID _id;
        std::unique_ptr<std::string> _name;
    };

    using Container = std::vector<Pair>;
    using NameMap = std::unordered_map<std::string_view, size_t>;
    using IDMap = std::unordered_map<EdgeTypeID, size_t>;

    EdgeTypeMap();
    ~EdgeTypeMap();

    EdgeTypeMap(const EdgeTypeMap&);
    EdgeTypeMap(EdgeTypeMap&&) noexcept = default;
    EdgeTypeMap& operator=(const EdgeTypeMap&);
    EdgeTypeMap& operator=(EdgeTypeMap&&) noexcept = default;

    [[nodiscard]] std::optional<EdgeTypeID> get(const std::string& name) const;
    [[nodiscard]] std::optional<std::string_view> getName(EdgeTypeID id) const;
    [[nodiscard]] size_t getCount() const;

    [[nodiscard]] Container::const_iterator begin() const { return _container.begin(); }
    [[nodiscard]] Container::const_iterator end() const { return _container.end(); }

    EdgeTypeID getOrCreate(const std::string& edgeTypeName);

private:
    Container _container;
    NameMap _nameMap;
    IDMap _idMap;
};

}
