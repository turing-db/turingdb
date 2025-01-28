#pragma once

#include <string>
#include <unordered_map>

#include "EntityID.h"
#include "RWSpinLock.h"

namespace db {

class EdgeTypeMap {
public:
    using NameMap = std::unordered_map<std::string, EdgeTypeID>;
    using IDMap = std::unordered_map<EdgeTypeID, std::string_view>;

    EdgeTypeMap();
    ~EdgeTypeMap();

    EdgeTypeMap(const EdgeTypeMap&) = delete;
    EdgeTypeMap(EdgeTypeMap&&) = delete;
    EdgeTypeMap& operator=(const EdgeTypeMap&) = delete;
    EdgeTypeMap& operator=(EdgeTypeMap&&) = delete;

    [[nodiscard]] EdgeTypeID get(const std::string& name) const;
    [[nodiscard]] std::string_view getName(EdgeTypeID id) const;
    [[nodiscard]] size_t getCount() const;

    EdgeTypeID getOrCreate(const std::string& name);
    bool tryCreate(std::string&& name);
    EdgeTypeID create(std::string name);

private:
    mutable RWSpinLock _lock;
    NameMap _nameMap;
    IDMap _idMap;

    bool unsafeExists(const std::string& name) const;
    EdgeTypeID unsafeCreate(std::string name);
};

}
