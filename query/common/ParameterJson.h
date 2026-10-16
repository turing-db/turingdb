#pragma once

#include <nlohmann/json_fwd.hpp>

namespace db {

class ParameterMap;
class ParameterValue;

void readJsonParameter(const nlohmann::json& json, ParameterValue& value);
void readJsonParameters(const nlohmann::json& json, ParameterMap& parameters);

}
