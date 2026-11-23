#include "PyValueConversion.h"

#include <span>

#include "PyGraphPath.h"

using namespace pybindings;

nb::object ValueToPyObject::duration(db::Duration value) const {
    return _times.makeTimedelta(value.getMicroseconds());
}

nb::object ValueToPyObject::dateTime(db::DateTime value) const {
    return _times.makeTimestamp(value.getMicroseconds());
}

nb::object ValueToPyObject::point(const db::Point& value) const {
    if (!_point.is_valid()) {
        _point = nb::module_::import_("turingdb.point").attr("Point");
    }

    const db::PointCRS crs = value.getCRS();
    const nb::object z = db::Point::getDimension(crs) == 3 ? nb::cast(value.getZ()) : nb::none();

    return _point(db::Point::getSRID(crs), value.getX(), value.getY(), z);
}

nb::object ValueToPyObject::path(std::span<const uint64_t> ids) const {
    if (ids.empty()) {
        return nb::none();
    }

    size_t offset = 0;
    const PyOwnerBlocks<uint64_t>::Block& block = _pathIDs.place(ids, offset);

    return PyGraphPath::create(block._array, block._data, offset, (ids.size() + 1) / 2);
}

nb::object ValueToPyObject::embedding(std::span<const float> values) const {
    size_t offset = 0;
    const PyOwnerBlocks<float>::Block& block = _embeddings.place(values, offset);

    const nb::slice range(offset, offset + values.size());
    return block._array[range];
}

const nb::str& ValueToPyObject::mapKey(std::string_view key) const {
    const auto found = _mapKeys.find(key);
    if (found != _mapKeys.end()) {
        return found->second;
    }

    return _mapKeys.emplace(key, toPyString(key)).first->second;
}

// The list is untracked by the garbage collector until every slot is set, so no
// gc.get_objects() run by an element's conversion can hand Python a list holding NULL
nb::object ValueToPyObject::view(const db::ListView& listView) const {
    const std::span<const db::CellView> elements = listView.elements();

    PyObject* const list = PyList_New(static_cast<Py_ssize_t>(elements.size()));
    if (!list) {
        throw nb::python_error();
    }

    nb::object out = nb::steal(list);
    PyObject_GC_UnTrack(list);

    for (size_t index = 0; index < elements.size(); index++) {
        PyList_SET_ITEM(list, static_cast<Py_ssize_t>(index), element(elements[index]).release().ptr());
    }

    PyObject_GC_Track(list);
    return out;
}

nb::object ValueToPyObject::element(const db::CellView element) const {
    return db::CellTagDispatcher {element.getTag()}.execute(*this, element);
}

nb::object ValueToPyObject::view(const db::MapView& mapView) const {
    nb::dict out;
    for (const db::MapEntryView entry : mapView.entries()) {
        out[mapKey(entry.getKey())] = this->entry(entry);
    }
    return out;
}

nb::object ValueToPyObject::entry(const db::MapEntryView entry) const {
    return element(entry.getValue());
}
