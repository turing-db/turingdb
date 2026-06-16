#include <exception>
#include <string>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include "PyTuringClient.h"

#include "TuringException.h"

namespace nb = nanobind;

NB_MODULE(_turingproto, m) {
    using pybindings::PyTuringClient;

    m.doc() = "TuringDB Binary Protocol Python bindings (nanobind)";

    nb::class_<PyTuringClient>(m, "TuringProtoClient")
        .def(nb::init<const std::string&, const std::string&>(),
             nb::arg("host"), nb::arg("port"))
        .def("connect", &PyTuringClient::connect)
        .def("disconnect", &PyTuringClient::disconnect)
        .def("is_connected", &PyTuringClient::isConnected)
        .def("set_remote_address", &PyTuringClient::setRemoteAddress, nb::arg("address"))
        .def("set_remote_port", &PyTuringClient::setRemotePort, nb::arg("port"))
        .def("set_graph_name", &PyTuringClient::setGraphName, nb::arg("name"))
        .def("set_auth_token", &PyTuringClient::setAuthToken, nb::arg("token"))
        .def("set_change_id", &PyTuringClient::setChangeID, nb::arg("change_id"))
        .def("clear_change_id", &PyTuringClient::clearChangeID)
        .def("set_commit_hash", &PyTuringClient::setCommitHash, nb::arg("commit_hash"))
        .def("clear_commit_hash", &PyTuringClient::clearCommitHash)
        .def("query", &PyTuringClient::query, nb::arg("cypher"));

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
