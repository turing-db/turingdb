#include "PyParameters.h"

#include <string>
#include <utility>
#include <vector>

#include <nanobind/stl/string.h>

#include "ParameterMap.h"
#include "ParameterValue.h"

#include "TuringException.h"

namespace pybindings {

namespace {

using Item = std::pair<nb::handle, db::ParameterValue*>;

std::string getTypeName(nb::handle value) {
    return nb::type_name(value.type()).c_str();
}

std::string getKey(nb::handle key) {
    if (!nb::isinstance<nb::str>(key)) {
        throw TuringException("A parameter name or map key must be a str, not " + getTypeName(key));
    }

    return nb::cast<std::string>(key);
}

// A bool is checked before an int: in Python, bool is a subclass of int.
void readValue(nb::handle source, db::ParameterValue& target, std::vector<Item>& worklist) {
    if (source.is_none()) {
        target.setNull();
    } else if (nb::isinstance<nb::bool_>(source)) {
        target.setBool(nb::cast<bool>(source));
    } else if (nb::isinstance<nb::int_>(source)) {
        int overflow = 0;
        const long long number = PyLong_AsLongLongAndOverflow(source.ptr(), &overflow);
        if (overflow != 0) {
            throw TuringException("Integer " + std::string(nb::str(source).c_str()) + " exceeds the Int64 range");
        }

        target.setInt64(number);
    } else if (nb::isinstance<nb::float_>(source)) {
        target.setDouble(nb::cast<double>(source));
    } else if (nb::isinstance<nb::str>(source)) {
        target.setString(nb::cast<std::string>(source));
    } else if (nb::isinstance<nb::list>(source) || nb::isinstance<nb::tuple>(source)) {
        db::ParameterValue::List& list = target.setList();
        list.resize(nb::len(source));

        size_t index = 0;
        for (nb::handle element : source) {
            worklist.emplace_back(element, &list[index]);
            index++;
        }
    } else if (nb::isinstance<nb::dict>(source)) {
        db::ParameterValue::Map& map = target.setMap();

        for (const auto& [key, value] : nb::borrow<nb::dict>(source)) {
            worklist.emplace_back(value, &map[getKey(key)]);
        }
    } else {
        throw TuringException("A parameter cannot hold a Python " + getTypeName(source));
    }
}

}

void readParameters(nb::handle source, db::ParameterMap& parameters) {
    if (!nb::isinstance<nb::dict>(source)) {
        throw TuringException("The query parameters must be a dict, not " + getTypeName(source));
    }

    std::vector<Item> worklist;
    for (const auto& [name, value] : nb::borrow<nb::dict>(source)) {
        worklist.emplace_back(value, &parameters.insert(getKey(name)));
    }

    while (!worklist.empty()) {
        const auto [value, target] = worklist.back();
        worklist.pop_back();

        readValue(value, *target, worklist);
    }
}

}
