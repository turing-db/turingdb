#pragma once

#include <nlohmann/json_fwd.hpp>

namespace db {

class ParameterValue;

void readJsonParameter(const nlohmann::json& json, ParameterValue& value);

}
