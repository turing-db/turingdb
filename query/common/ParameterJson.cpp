#include "ParameterJson.h"

#include <stdint.h>
#include <limits>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/fmt/fmt.h>

#include "ParameterMap.h"
#include "ParameterValue.h"

#include "TuringException.h"

using namespace db;

void db::readJsonParameter(const nlohmann::json& json, ParameterValue& value) {
    using Item = std::pair<const nlohmann::json*, ParameterValue*>;

    std::vector<Item> worklist;
    worklist.emplace_back(&json, &value);

    while (!worklist.empty()) {
        const auto [current, target] = worklist.back();
        worklist.pop_back();

        if (current->is_null()) {
            target->setNull();
        } else if (current->is_boolean()) {
            target->setBool(current->get<bool>());
        } else if (current->is_number_unsigned()) {
            const uint64_t number = current->get<uint64_t>();
            if (number > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                throw TuringException(fmt::format("Integer {} exceeds the Int64 range", number));
            }

            target->setInt64(static_cast<int64_t>(number));
        } else if (current->is_number_integer()) {
            target->setInt64(current->get<int64_t>());
        } else if (current->is_number_float()) {
            target->setDouble(current->get<double>());
        } else if (current->is_string()) {
            target->setString(current->get_ref<const std::string&>());
        } else if (current->is_array()) {
            ParameterValue::List& list = target->setList();
            list.resize(current->size());

            for (size_t index = 0; index < list.size(); index++) {
                worklist.emplace_back(&(*current)[index], &list[index]);
            }
        } else {
            ParameterValue::Map& map = target->setMap();
            for (const auto& [key, entry] : current->items()) {
                worklist.emplace_back(&entry, &map[key]);
            }
        }
    }
}

void db::readJsonParameters(const nlohmann::json& json, ParameterMap& parameters) {
    if (!json.is_object()) {
        throw TuringException("The parameters must be a JSON object");
    }

    for (const auto& [name, entry] : json.items()) {
        ParameterValue value;
        try {
            readJsonParameter(entry, value);
        } catch (const TuringException& e) {
            throw TuringException(fmt::format("Invalid value for parameter ${}: {}", name, e.what()));
        }

        parameters.set(name, value);
    }
}
