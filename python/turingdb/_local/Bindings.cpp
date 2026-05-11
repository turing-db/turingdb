#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include <exception>
#include <string>

#include "PyTuringDB.h"
#include "TuringException.h"

namespace nb = nanobind;

NB_MODULE(_turinglocal, m) {
    using pybindings::PyTuringDB;

    m.doc() = "TuringDB in-process Python bindings (nanobind)";

    nb::class_<PyTuringDB>(m, "PyTuringDB")
        .def(nb::init<>())
        .def(nb::init<const std::string&>(), nb::arg("data_dir"))
        .def("set_graph_name", &PyTuringDB::setGraphName, nb::arg("name"))
        .def("get_graph_name", &PyTuringDB::getGraphName)
        .def("set_change_id", &PyTuringDB::setChangeID, nb::arg("change_id"))
        .def("clear_change_id", &PyTuringDB::clearChangeID)
        .def("set_commit_hash", &PyTuringDB::setCommitHash, nb::arg("commit_hash"))
        .def("clear_commit_hash", &PyTuringDB::clearCommitHash)
        .def("query", &PyTuringDB::query, nb::arg("cypher"));

    nb::register_exception_translator(
        [](const std::exception_ptr& p, void*) {
            try {
                std::rethrow_exception(p);
            } catch (const TuringException& e) {
                PyErr_SetString(PyExc_RuntimeError, e.what());
            } catch (const std::exception& e) {
                PyErr_SetString(PyExc_RuntimeError, e.what());
            }
        });
}
