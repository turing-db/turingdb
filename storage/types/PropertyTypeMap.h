#pragma once

#include <string>
#include <unordered_map>

#include "EntityID.h"
#include "PropertyType.h"
#include "RWSpinLock.h"

namespace db {

class GraphReport;

class PropertyTypeMap {
public:
    using OffsetMap = std::unordered_map<std::string, size_t>;
    using IDMap = std::unordered_map<PropertyTypeID, std::string_view>;
    using PropertyTypes = std::vector<PropertyType>;

    PropertyTypeMap();
    ~PropertyTypeMap();

    PropertyTypeMap(const PropertyTypeMap&) = delete;
    PropertyTypeMap(PropertyTypeMap&&) = delete;
    PropertyTypeMap& operator=(const PropertyTypeMap&) = delete;
    PropertyTypeMap& operator=(PropertyTypeMap&&) = delete;

    [[nodiscard]] PropertyType get(const std::string& name) const;
    [[nodiscard]] PropertyType get(PropertyTypeID id) const;
    [[nodiscard]] std::string_view getName(PropertyTypeID id) const;
    [[nodiscard]] size_t getCount() const;

    PropertyType getOrCreate(const std::string& name, ValueType value);
    bool tryCreate(std::string&& name, ValueType valueType);
    PropertyType create(std::string name, ValueType valueType);

private:
    friend GraphReport;

    mutable RWSpinLock _lock;
    uint64_t _nextFreeID {0};
    OffsetMap _offsetMap;
    IDMap _idMap;
    PropertyTypes _propTypes;

    PropertyType unsafeCreate(std::string name, ValueType valueType);
    [[nodiscard]] bool unsafeExists(const std::string& name) const;
};

}
