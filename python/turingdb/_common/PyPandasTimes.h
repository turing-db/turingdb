#pragma once

#include <stdint.h>

#include <nanobind/nanobind.h>

namespace pybindings {

namespace nb = nanobind;

// pandas' Timestamp and Timedelta, which give a DateTime and a Duration the type of a row of
// their column: a Timestamp in UTC and a Timedelta, both in microseconds. Loaded on first use.
class PyPandasTimes {
public:
    PyPandasTimes();
    ~PyPandasTimes();

    nb::object makeTimestamp(int64_t microseconds);
    nb::object makeTimedelta(int64_t microseconds);

private:
    nb::object _timestamp;
    nb::object _timedelta;
    nb::object _unit;
    nb::object _utc;
    nb::object _timestampKeywords;
    nb::object _timedeltaKeywords;

    void load();
};

}
