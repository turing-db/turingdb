#pragma once

#include <stdint.h>
#include <map>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "metadata/PropertyType.h"

namespace db {

class ParameterValue {
public:
    using List = std::vector<ParameterValue>;
    using Map = std::map<std::string, ParameterValue>;

    ParameterValue();
    ~ParameterValue();

    bool isNull() const;
    ValueType getType() const;

    int64_t getInt64() const;
    double getDouble() const;
    bool getBool() const;
    const std::string& getString() const;
    const List& getList() const;
    const Map& getMap() const;

    void setNull();
    void setInt64(int64_t value);
    void setDouble(double value);
    void setBool(bool value);
    void setString(std::string_view value);
    List& setList();
    Map& setMap();

private:
    std::variant<std::monostate, int64_t, double, bool, std::string, List, Map> _value;
};

}
