#pragma once

#include <optional>
#include <string>
#include <unordered_map>

#include "metadata/PropertyType.h"

namespace db {

class PropertyTypeMap {
public:
    struct Pair {
        PropertyType _pt;
        std::unique_ptr<std::string> _name;
    };

    using Container = std::vector<Pair>;
    using NameMap = std::unordered_map<std::string_view, size_t>;
    using IDMap = std::unordered_map<PropertyTypeID, size_t>;

    PropertyTypeMap();
    ~PropertyTypeMap();

    PropertyTypeMap(const PropertyTypeMap&);
    PropertyTypeMap(PropertyTypeMap&&) noexcept = default;
    PropertyTypeMap& operator=(const PropertyTypeMap&);
    PropertyTypeMap& operator=(PropertyTypeMap&&) noexcept = default;

    [[nodiscard]] std::optional<PropertyType> get(PropertyTypeID ptID) const;
    [[nodiscard]] std::optional<PropertyType> get(std::string_view name) const;
    [[nodiscard]] std::optional<std::string_view> getName(PropertyTypeID ptID) const;
    [[nodiscard]] size_t getCount() const;

    [[nodiscard]] Container::const_iterator begin() const { return _container.begin(); }
    [[nodiscard]] Container::const_iterator end() const { return _container.end(); }

    PropertyType getOrCreate(std::string_view labelName, ValueType valueType);

private:
    Container _container;
    NameMap _nameMap;
    IDMap _idMap;
};

}
