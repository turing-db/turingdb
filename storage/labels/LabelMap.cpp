#include "LabelMap.h"
#include "BioAssert.h"

#include <mutex>
#include <shared_mutex>

using namespace db;

LabelMap::LabelMap() = default;

LabelMap::~LabelMap() = default;

LabelID LabelMap::get(const std::string& name) const {
    std::shared_lock guard(_lock);
    auto it = _nameMap.find(name);
    if (it == _nameMap.end()) {
        return LabelID {};
    }
    return it->second;
}

std::string_view LabelMap::getName(LabelID id) const {
    std::shared_lock guard(_lock);
    auto it = _idMap.find(id);
    msgbioassert(it != _idMap.end(),
                 "Label does not exist");
    return it->second;
}

size_t LabelMap::getCount() const {
    std::shared_lock guard(_lock);
    return _nameMap.size();
}

LabelID LabelMap::getOrCreate(const std::string& name) {
    std::unique_lock guard(_lock);

    if (!unsafeExists(name)) {
        return unsafeCreate(name);
    }

    return _nameMap.at(name);
}

bool LabelMap::tryCreate(std::string&& name) {
    std::unique_lock guard(_lock);
    if (!unsafeExists(name)) {
        unsafeCreate(std::move(name));
        return true;
    }

    return false;
}

LabelID LabelMap::create(std::string name) {
    std::unique_lock guard(_lock);
    return unsafeCreate(std::move(name));
}

LabelID LabelMap::unsafeCreate(std::string name) {
    msgbioassert(_nameMap.size() < std::numeric_limits<LabelID::Type>::max(),
                 "Too many labels registered");

    const auto count = static_cast<LabelID::Type>(_nameMap.size());
    const LabelID nextID {count};
    auto it = _nameMap.emplace(std::move(name), nextID);
    _idMap.emplace(nextID, it.first->first);
    return nextID;
}

bool LabelMap::unsafeExists(const std::string& name) const {
    return _nameMap.find(name) != _nameMap.end();
}


