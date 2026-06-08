#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include <exception>
#include <string>

#include "PyTuringEmbedded.h"
#include "TuringException.h"

namespace nb = nanobind;

NB_MODULE(_turingembedded, m) {
    using pybindings::PyTuringEmbedded;

    m.doc() = "TuringDB in-process Python bindings (nanobind)";

    nb::class_<PyTuringEmbedded>(m, "PyTuringEmbedded")
        .def(nb::init<>())
        .def(nb::init<const std::string&>(), nb::arg("data_dir"))
        .def("set_graph_name", &PyTuringEmbedded::setGraphName, nb::arg("name"))
        .def("get_graph_name", &PyTuringEmbedded::getGraphName)
        .def("set_change_id", &PyTuringEmbedded::setChangeID, nb::arg("change_id"))
        .def("clear_change_id", &PyTuringEmbedded::clearChangeID)
        .def("set_commit_hash", &PyTuringEmbedded::setCommitHash, nb::arg("commit_hash"))
        .def("clear_commit_hash", &PyTuringEmbedded::clearCommitHash)
        .def("query", &PyTuringEmbedded::query, nb::arg("cypher"));

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
