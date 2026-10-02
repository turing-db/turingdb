#include "NumpyStringArray.h"

#define NPY_NO_DEPRECATED_API NPY_2_0_API_VERSION
#define NPY_TARGET_VERSION NPY_2_0_API_VERSION
#include <numpy/arrayobject.h>
#include <numpy/npy_2_compat.h>

using namespace pybindings;

namespace {

// getRow(row) returns the row's string, or nullptr for a missing value.
template <typename GetRow>
nb::object packStrings(size_t rowCount, const GetRow& getRow) {
    if (PyArray_ImportNumPyAPI() < 0) {
        throw nb::python_error();
    }

    const nb::object stringDType = nb::module_::import_("numpy.dtypes").attr("StringDType");
    nb::object descriptor = stringDType(nb::arg("na_object") = nb::none());

    npy_intp dimensions[1] = {static_cast<npy_intp>(rowCount)};
    nb::object array = nb::steal(PyArray_NewFromDescr(&PyArray_Type,
                                                       reinterpret_cast<PyArray_Descr*>(descriptor.release().ptr()),
                                                       1,
                                                       dimensions,
                                                       nullptr,
                                                       nullptr,
                                                       0,
                                                       nullptr));
    if (!array.is_valid()) {
        throw nb::python_error();
    }

    PyArrayObject* arrayObject = reinterpret_cast<PyArrayObject*>(array.ptr());
    char* rows = PyArray_BYTES(arrayObject);
    const npy_intp itemSize = PyArray_ITEMSIZE(arrayObject);

    npy_string_allocator* allocator = NpyString_acquire_allocator(reinterpret_cast<PyArray_StringDTypeObject*>(PyArray_DESCR(arrayObject)));

    for (size_t row = 0; row < rowCount; row++) {
        npy_packed_static_string* packed = reinterpret_cast<npy_packed_static_string*>(rows + row * itemSize);
        const std::string_view* value = getRow(row);

        const int result = value ? NpyString_pack(allocator, packed, value->data(), value->size())
                                 : NpyString_pack_null(allocator, packed);
        if (result < 0) {
            NpyString_release_allocator(allocator);
            throw nb::python_error();
        }
    }

    NpyString_release_allocator(allocator);

    return array;
}

}

nb::object pybindings::makeStringArray(std::span<const std::string_view> rows) {
    return packStrings(rows.size(), [rows](size_t row) {
        return &rows[row];
    });
}

nb::object pybindings::makeStringArray(std::span<const std::optional<std::string_view>> rows) {
    return packStrings(rows.size(), [rows](size_t row) -> const std::string_view* {
        const std::optional<std::string_view>& value = rows[row];
        return value ? &*value : nullptr;
    });
}

nb::object pybindings::makeStringArray(const std::optional<std::string_view>& value, size_t rowCount) {
    const std::string_view* row = value ? &*value : nullptr;

    return packStrings(rowCount, [row](size_t) {
        return row;
    });
}
