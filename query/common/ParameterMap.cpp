#include "ParameterMap.h"

using namespace db;

ParameterMap::ParameterMap()
{
}

ParameterMap::~ParameterMap() {
}

void ParameterMap::set(std::string_view name, const ParameterValue& value) {
    _values.insert_or_assign(std::string(name), value);
}

const ParameterValue* ParameterMap::get(std::string_view name) const {
    const auto it = _values.find(name);
    if (it == _values.end()) {
        return nullptr;
    }

    return &it->second;
}

void ParameterMap::clear() {
    _values.clear();
}
