#include "TypedPropertySpec.h"

using namespace db;

ValueType TypedPropertySpec::getType(std::string_view propertyName) const {
    const auto it = _types.find(propertyName);
    if (it == _types.end()) {
        return ValueType::Invalid;
    }

    return it->second;
}

bool TypedPropertySpec::contains(std::string_view propertyName) const {
    return _types.contains(propertyName);
}

void TypedPropertySpec::emplace(std::string_view propertyName, ValueType type) {
    const auto [it, inserted] = _types.emplace(propertyName, type);

    if (!inserted && it->second != type) {
        _clashes.push_back(Clash {._name = propertyName, ._first = it->second, ._second = type});
    }
}
