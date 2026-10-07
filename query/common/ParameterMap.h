#pragma once

#include <string>
#include <string_view>

#include "ParameterValue.h"

#include "StringHashMap.h"

namespace db {

class ParameterMap {
public:
    using Values = StringHashMap<std::string, ParameterValue>;

    ParameterMap();
    ~ParameterMap();

    void set(std::string_view name, const ParameterValue& value);
    const ParameterValue* get(std::string_view name) const;
    void clear();

    bool empty() const { return _values.empty(); }
    const Values& getValues() const { return _values; }

private:
    Values _values;
};

}
