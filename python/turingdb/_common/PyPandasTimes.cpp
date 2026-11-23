#include "PyPandasTimes.h"

#include <span>

using namespace pybindings;

namespace {

// numpy's NPY_FR_us, the resolution of a value in microseconds
constexpr int NUMPY_MICROSECONDS = 9;

// keywords, when it is set, names the last arguments
nb::object callWithKeywords(const nb::object& callable, std::span<PyObject* const> arguments, const nb::object& keywords) {
    const size_t keywordCount = keywords.is_valid() ? static_cast<size_t>(PyTuple_GET_SIZE(keywords.ptr())) : 0;

    PyObject* const result = PyObject_Vectorcall(callable.ptr(), arguments.data(), arguments.size() - keywordCount, keywords.ptr());
    if (!result) {
        throw nb::python_error();
    }

    return nb::steal(result);
}

}

PyPandasTimes::PyPandasTimes() {
}

PyPandasTimes::~PyPandasTimes() {
}

nb::object PyPandasTimes::makeTimestamp(int64_t microseconds) {
    load();

    const nb::int_ value(microseconds);
    PyObject* const arguments[] = {value.ptr(), _unit.ptr(), _utc.ptr()};

    return callWithKeywords(_timestamp, arguments, _timestampKeywords);
}

nb::object PyPandasTimes::makeTimedelta(int64_t microseconds) {
    load();

    const nb::int_ value(microseconds);
    PyObject* const arguments[] = {value.ptr(), _unit.ptr()};

    return callWithKeywords(_timedelta, arguments, _timedeltaKeywords);
}

// _from_value_and_reso is how pandas makes a Timestamp or a Timedelta of a value it holds,
// nearly 3 times as fast as Timestamp(value, unit="us", tz=utc), which parses its arguments.
// It is private, so a pandas that lacks it gets the public constructors.
void PyPandasTimes::load() {
    if (_timestamp.is_valid()) {
        return;
    }

    const nb::module_ pandas = nb::module_::import_("pandas");
    const nb::object timestamp = pandas.attr("Timestamp");
    const nb::object timedelta = pandas.attr("Timedelta");

    const bool hasValueConstructors = nb::hasattr(timestamp, "_from_value_and_reso")
                                   && nb::hasattr(timedelta, "_from_value_and_reso");

    _utc = nb::module_::import_("datetime").attr("timezone").attr("utc");

    if (hasValueConstructors) {
        _unit = nb::int_(NUMPY_MICROSECONDS);
        _timedelta = timedelta.attr("_from_value_and_reso");
        _timestamp = timestamp.attr("_from_value_and_reso");
    } else {
        _unit = nb::str("us");
        _timestampKeywords = nb::make_tuple("unit", "tz");
        _timedeltaKeywords = nb::make_tuple("unit");
        _timedelta = timedelta;
        _timestamp = timestamp;
    }
}
