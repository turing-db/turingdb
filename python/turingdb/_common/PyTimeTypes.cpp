#include "PyTimeTypes.h"

#include <utility>

using namespace pybindings;

PyTimeTypes::PyTimeTypes() {
}

PyTimeTypes::~PyTimeTypes() {
}

const nb::object& PyTimeTypes::getTimedelta() {
    load();
    return _timedelta;
}

const nb::object& PyTimeTypes::getDatetime() {
    load();
    return _datetime;
}

const nb::object& PyTimeTypes::getEpoch() {
    load();
    return _epoch;
}

void PyTimeTypes::load() {
    if (_timedelta.is_valid()) {
        return;
    }

    const nb::module_ datetime = nb::module_::import_("datetime");
    nb::object timedelta = datetime.attr("timedelta");
    nb::object datetimeType = datetime.attr("datetime");
    nb::object epoch = datetimeType(1970, 1, 1, nb::arg("tzinfo") = datetime.attr("timezone").attr("utc"));

    _epoch = std::move(epoch);
    _datetime = std::move(datetimeType);
    _timedelta = std::move(timedelta);
}
