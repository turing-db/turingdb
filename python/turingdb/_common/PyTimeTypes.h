#pragma once

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace pybindings {

// The datetime module's timedelta and datetime, and the UTC epoch, loaded on first use
class PyTimeTypes {
public:
    PyTimeTypes();
    ~PyTimeTypes();

    const nb::object& getTimedelta();
    const nb::object& getDatetime();
    const nb::object& getEpoch();

private:
    nb::object _timedelta;
    nb::object _datetime;
    nb::object _epoch;

    void load();
};

}
