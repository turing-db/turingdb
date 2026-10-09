#include "ParameterTestHelpers.h"

#include "ParameterMap.h"
#include "ParameterValue.h"

using namespace db;

void turing::test::setString(ParameterMap& parameters, std::string_view name, std::string_view value) {
    ParameterValue parameter;
    parameter.setString(value);
    parameters.set(name, parameter);
}

void turing::test::setStrings(ParameterMap& parameters, std::string_view name, std::span<const std::string_view> values) {
    ParameterValue parameter;
    ParameterValue::List& list = parameter.setList();
    for (const std::string_view value : values) {
        list.emplace_back().setString(value);
    }

    parameters.set(name, parameter);
}
