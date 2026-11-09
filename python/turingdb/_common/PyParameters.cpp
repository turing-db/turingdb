#include "PyParameters.h"

#include <stdint.h>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include <nanobind/ndarray.h>
#include <nanobind/stl/string.h>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "PyTimeTypes.h"
#include "metadata/DateTime.h"
#include "metadata/Duration.h"

#include "TuringException.h"

namespace pybindings {

namespace {

using Item = std::pair<nb::handle, db::ParameterValue*>;

// The array an Embedding is sent as. A wider float would have to be narrowed, so a float64
// array is not one.
using Float32Vector = nb::ndarray<const float, nb::ndim<1>, nb::device::cpu>;

// Whether @param value != value holds, as it does for pandas.NaT. PyObject_RichCompareBool,
// and so nanobind's equal(), answer by identity first and never ask.
bool isUnequalToItself(nb::handle value) {
    const nb::object unequal = nb::steal(PyObject_RichCompare(value.ptr(), value.ptr(), Py_NE));
    if (!unequal.is_valid()) {
        throw nb::python_error();
    }

    return nb::cast<bool>(unequal);
}

int64_t microsecondsOf(nb::handle delta) {
    constexpr int64_t MICROSECONDS_PER_DAY = 86'400'000'000;
    constexpr int64_t MAX_DAYS = std::numeric_limits<int64_t>::max() / MICROSECONDS_PER_DAY - 1;

    const int64_t days = nb::cast<int64_t>(delta.attr("days"));
    const int64_t seconds = nb::cast<int64_t>(delta.attr("seconds"));
    const int64_t microseconds = nb::cast<int64_t>(delta.attr("microseconds"));

    if (days > MAX_DAYS || days < -MAX_DAYS) {
        throw TuringException("A timedelta of " + std::to_string(days) + " days exceeds the Duration range");
    }

    const bool hasNanoseconds = nb::hasattr(delta, "nanoseconds") && nb::cast<int64_t>(delta.attr("nanoseconds")) != 0;
    if (hasNanoseconds) {
        throw TuringException("A parameter cannot hold nanoseconds: a Duration and a DateTime count microseconds");
    }

    return (days * 86'400 + seconds) * 1'000'000 + microseconds;
}

std::string getTypeName(nb::handle value) {
    return nb::type_name(value.type()).c_str();
}

std::string getKey(nb::handle key) {
    if (!nb::isinstance<nb::str>(key)) {
        throw TuringException("A parameter name or map key must be a str, not " + getTypeName(key));
    }

    return nb::cast<std::string>(key);
}

bool isPoint(nb::handle value, nb::object& pointType) {
    if (!pointType.is_valid()) {
        pointType = nb::module_::import_("turingdb.point").attr("Point");
    }

    const int isInstance = PyObject_IsInstance(value.ptr(), pointType.ptr());
    if (isInstance < 0) {
        throw nb::python_error();
    }

    return isInstance == 1;
}

void readEmbedding(const Float32Vector& source, db::ParameterValue& target) {
    const auto floats = source.view();

    db::ParameterValue::Embedding& embedding = target.setEmbedding();
    embedding.resize(floats.shape(0));
    for (size_t index = 0; index < embedding.size(); index++) {
        embedding[index] = floats(index);
    }
}

// A bool is checked before an int: in Python, bool is a subclass of int. A Point is a tuple
// too, but a value rather than a list, and no parameter holds a point.
void readValue(nb::handle source,
               db::ParameterValue& target,
               PyTimeTypes& times,
               nb::object& pointType,
               std::vector<Item>& worklist) {
    const bool readsAsAList = nb::isinstance<nb::list>(source)
                           || (nb::isinstance<nb::tuple>(source) && !isPoint(source, pointType));

    Float32Vector floats;

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
    } else if (readsAsAList) {
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
    } else if (nb::isinstance(source, times.getTimedelta())) {
        target.setDuration(db::Duration {microsecondsOf(source)});
    } else if (nb::isinstance(source, times.getDatetime())) {
        // pandas.NaT is a datetime unequal to itself: the missing instant, as None is
        if (isUnequalToItself(source)) {
            target.setNull();
        } else if (source.attr("utcoffset")().is_none()) {
            // What datetime itself calls naive: no tzinfo, or one that names no offset
            throw TuringException("A datetime parameter must carry a timezone: a naive datetime names no instant");
        } else {
            target.setDateTime(db::DateTime {microsecondsOf(source - times.getEpoch())});
        }
    } else if (nb::try_cast(source, floats, false)) {
        readEmbedding(floats, target);
    } else if (nb::hasattr(source, "dtype")) {
        throw TuringException("A parameter cannot hold a Python " + getTypeName(source) + " of "
                              + nb::cast<std::string>(source.attr("dtype").attr("name"))
                              + ": an Embedding is a 1-dimensional float32 array");
    } else {
        throw TuringException("A parameter cannot hold a Python " + getTypeName(source));
    }
}

}

void readParameters(nb::handle source, db::ParameterMap& parameters) {
    if (!nb::isinstance<nb::dict>(source)) {
        throw TuringException("The query parameters must be a dict, not " + getTypeName(source));
    }

    PyTimeTypes times;
    nb::object pointType;

    std::vector<Item> worklist;
    for (const auto& [name, value] : nb::borrow<nb::dict>(source)) {
        worklist.emplace_back(value, &parameters.insert(getKey(name)));
    }

    while (!worklist.empty()) {
        const auto [value, target] = worklist.back();
        worklist.pop_back();

        readValue(value, *target, times, pointType, worklist);
    }
}

}
