#pragma once

#include <span>
#include <string_view>

namespace db {
class ParameterMap;
}

namespace turing::test {

void setString(db::ParameterMap& parameters, std::string_view name, std::string_view value);
void setStrings(db::ParameterMap& parameters, std::string_view name, std::span<const std::string_view> values);

}
