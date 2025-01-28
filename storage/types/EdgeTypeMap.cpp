#include "EdgeTypeMap.h"
#include "BioAssert.h"

#include <mutex>
#include <shared_mutex>

using namespace db;

EdgeTypeMap::EdgeTypeMap() = default;

EdgeTypeMap::~EdgeTypeMap() = default;

EdgeTypeID EdgeTypeMap::get(const std::string& name) const {
    std::shared_lock guard(_lock);
    auto it = _nameMap.find(name);
    if (it == _nameMap.end()) {
        return EdgeTypeID {};
    }
    return it->second;
}

std::string_view EdgeTypeMap::getName(EdgeTypeID id) const {
    std::shared_lock guard(_lock);
    auto it = _idMap.find(id);
    msgbioassert(it != _idMap.end(),
                 "Edge type does not exist");
    return it->second;
}

size_t EdgeTypeMap::getCount() const {
    std::shared_lock guard(_lock);
    return _nameMap.size();
}

EdgeTypeID EdgeTypeMap::getOrCreate(const std::string& name) {
    std::unique_lock guard(_lock);

    if (!unsafeExists(name)) {
        return unsafeCreate(name);
    }

    return _nameMap.at(name);
}

bool EdgeTypeMap::tryCreate(std::string&& name) {
    std::unique_lock guard(_lock);
    if (!unsafeExists(name)) {
        unsafeCreate(std::move(name));
        return true;
    }

    return false;
}

EdgeTypeID EdgeTypeMap::create(std::string name) {
    std::unique_lock guard(_lock);
    return unsafeCreate(std::move(name));
}

EdgeTypeID EdgeTypeMap::unsafeCreate(std::string name) {
    msgbioassert(_nameMap.size() < std::numeric_limits<EdgeTypeID::Type>::max(),
                 "Too many edge types registered");

    const auto count = static_cast<EdgeTypeID::Type>(_nameMap.size());
    const EdgeTypeID nextID {count};
    auto it = _nameMap.emplace(std::move(name), nextID);
    _idMap.emplace(nextID, it.first->first);
    return nextID;
}

bool EdgeTypeMap::unsafeExists(const std::string& name) const {
    return _nameMap.find(name) != _nameMap.end();
}


