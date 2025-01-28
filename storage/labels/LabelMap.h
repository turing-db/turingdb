#pragma once

#include <string>
#include <unordered_map>

#include "EntityID.h"
#include "RWSpinLock.h"

namespace db {

class LabelMap {
public:
    using NameMap = std::unordered_map<std::string, LabelID>;
    using IDMap = std::unordered_map<LabelID, std::string_view>;

    LabelMap();
    ~LabelMap();

    LabelMap(const LabelMap&) = delete;
    LabelMap(LabelMap&&) = delete;
    LabelMap& operator=(const LabelMap&) = delete;
    LabelMap& operator=(LabelMap&&) = delete;

    [[nodiscard]] LabelID get(const std::string& name) const;
    [[nodiscard]] std::string_view getName(LabelID id) const;
    [[nodiscard]] size_t getCount() const;

    LabelID getOrCreate(const std::string& name);
    bool tryCreate(std::string&& name);
    LabelID create(std::string name);

private:
    mutable RWSpinLock _lock;
    NameMap _nameMap;
    IDMap _idMap;

    bool unsafeExists(const std::string& name) const;
    LabelID unsafeCreate(std::string name);
};

}
