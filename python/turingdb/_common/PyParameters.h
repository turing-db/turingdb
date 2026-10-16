#pragma once

#include <nanobind/nanobind.h>

namespace db {
class ParameterMap;
}

namespace pybindings {

namespace nb = nanobind;

// Reads a Python dict of query parameters
void readParameters(nb::handle source, db::ParameterMap& parameters);

}
