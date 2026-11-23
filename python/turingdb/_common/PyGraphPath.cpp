#include "PyGraphPath.h"

#include <string.h>
#include <exception>
#include <string>

#include "TuringException.h"

using namespace pybindings;

namespace {

struct GraphPathObject {
    PyObject_HEAD
    PyObject* _block {nullptr};
    const uint64_t* _ids {nullptr};
    Py_ssize_t _offset {0};
    Py_ssize_t _nodeCount {0};
};

PyTypeObject* graphPathType {nullptr};

GraphPathObject* asGraphPath(PyObject* object) {
    return reinterpret_cast<GraphPathObject*>(object);
}

Py_ssize_t idCountOf(const GraphPathObject* path) {
    return 2 * path->_nodeCount - 1;
}

template <typename Fn>
PyObject* callGuarded(Fn&& fn) {
    try {
        return fn().release().ptr();
    } catch (nb::python_error& error) {
        error.restore();
    } catch (const std::exception& error) {
        PyErr_SetString(PyExc_RuntimeError, error.what());
    }
    return nullptr;
}

PyObject* idView(PyObject* self, Py_ssize_t first) {
    return callGuarded([self, first]() {
        const GraphPathObject* path = asGraphPath(self);
        const nb::slice positions(path->_offset + first, path->_offset + idCountOf(path), Py_ssize_t {2});

        return nb::steal(PyObject_GetItem(path->_block, positions.ptr()));
    });
}

PyObject* getNodes(PyObject* self, void* closure) {
    return idView(self, 0);
}

PyObject* getEdges(PyObject* self, void* closure) {
    return idView(self, 1);
}

PyObject* getStart(PyObject* self, void* closure) {
    return PyLong_FromUnsignedLongLong(asGraphPath(self)->_ids[0]);
}

PyObject* getEnd(PyObject* self, void* closure) {
    const GraphPathObject* path = asGraphPath(self);
    return PyLong_FromUnsignedLongLong(path->_ids[idCountOf(path) - 1]);
}

Py_ssize_t length(PyObject* self) {
    return asGraphPath(self)->_nodeCount - 1;
}

int isTrue(PyObject* self) {
    return 1;
}

bool sameIDs(const GraphPathObject* path, const GraphPathObject* other) {
    if (path->_nodeCount != other->_nodeCount) {
        return false;
    }

    return memcmp(path->_ids, other->_ids, static_cast<size_t>(idCountOf(path)) * sizeof(uint64_t)) == 0;
}

PyObject* compare(PyObject* self, PyObject* other, int operation) {
    const bool comparable = PyObject_TypeCheck(other, graphPathType) && (operation == Py_EQ || operation == Py_NE);
    if (!comparable) {
        Py_RETURN_NOTIMPLEMENTED;
    }

    const bool equal = sameIDs(asGraphPath(self), asGraphPath(other));
    return PyBool_FromLong(equal == (operation == Py_EQ));
}

Py_hash_t hashIDs(PyObject* self) {
    const GraphPathObject* path = asGraphPath(self);
    const Py_ssize_t idCount = idCountOf(path);

    uint64_t value = static_cast<uint64_t>(idCount);
    for (Py_ssize_t position = 0; position < idCount; position++) {
        value = (value ^ path->_ids[position]) * 0x9E3779B97F4A7C15ULL;
        value ^= value >> 32;
    }

    const Py_hash_t result = static_cast<Py_hash_t>(value);
    if (result == -1) {
        return -2;
    }
    return result;
}

void appendIDs(std::string& text, const GraphPathObject* path, Py_ssize_t first) {
    text += '[';
    for (Py_ssize_t position = first; position < idCountOf(path); position += 2) {
        if (position != first) {
            text += ", ";
        }
        text += std::to_string(path->_ids[position]);
    }
    text += ']';
}

PyObject* represent(PyObject* self) {
    const GraphPathObject* path = asGraphPath(self);

    std::string text = "GraphPath(nodes=";
    appendIDs(text, path, 0);
    text += ", edges=";
    appendIDs(text, path, 1);
    text += ')';

    return PyUnicode_FromStringAndSize(text.data(), static_cast<Py_ssize_t>(text.size()));
}

void deallocate(PyObject* self) {
    PyTypeObject* type = Py_TYPE(self);

    Py_DECREF(asGraphPath(self)->_block);
    PyObject_Free(self);
    Py_DECREF(type);
}

PyGetSetDef properties[] = {
    {"nodes", getNodes, nullptr, "The node IDs, a read-only numpy uint64 view of the path's IDs", nullptr},
    {"edges", getEdges, nullptr, "The edge IDs, a read-only numpy uint64 view of the path's IDs", nullptr},
    {"start", getStart, nullptr, "The ID of the first node", nullptr},
    {"end", getEnd, nullptr, "The ID of the last node", nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},
};

char documentation[] = "A path of a query result: its nodes and edges, len() edges long";

PyType_Slot slots[] = {
    {Py_tp_doc, documentation},
    {Py_tp_dealloc, reinterpret_cast<void*>(deallocate)},
    {Py_tp_repr, reinterpret_cast<void*>(represent)},
    {Py_tp_hash, reinterpret_cast<void*>(hashIDs)},
    {Py_tp_richcompare, reinterpret_cast<void*>(compare)},
    {Py_tp_getset, properties},
    {Py_sq_length, reinterpret_cast<void*>(length)},
    {Py_nb_bool, reinterpret_cast<void*>(isTrue)},
    {0, nullptr},
};

PyType_Spec spec = {
    "turingdb.GraphPath",
    sizeof(GraphPathObject),
    0,
    Py_TPFLAGS_DEFAULT,
    slots,
};

}

void PyGraphPath::define(nb::module_& module) {
    PyObject* type = PyType_FromSpec(&spec);
    if (!type) {
        throw nb::python_error();
    }

    graphPathType = reinterpret_cast<PyTypeObject*>(type);
    graphPathType->tp_new = nullptr;

    module.attr("GraphPath") = nb::borrow(type);
}

void PyGraphPath::importFrom(const char* moduleName) {
    nb::object type = nb::module_::import_(moduleName).attr("GraphPath");

    const bool isGraphPathType = PyType_Check(type.ptr())
                              && reinterpret_cast<PyTypeObject*>(type.ptr())->tp_basicsize == sizeof(GraphPathObject);
    if (!isGraphPathType) {
        throw TuringException(std::string(moduleName) + ".GraphPath is not the GraphPath type");
    }

    graphPathType = reinterpret_cast<PyTypeObject*>(type.release().ptr());
}

nb::object PyGraphPath::create(const nb::object& block, const uint64_t* blockData, size_t offset, size_t nodeCount) {
    GraphPathObject* path = PyObject_New(GraphPathObject, graphPathType);
    if (!path) {
        throw nb::python_error();
    }

    path->_block = block.inc_ref().ptr();
    path->_ids = blockData + offset;
    path->_offset = static_cast<Py_ssize_t>(offset);
    path->_nodeCount = static_cast<Py_ssize_t>(nodeCount);

    return nb::steal(reinterpret_cast<PyObject*>(path));
}
