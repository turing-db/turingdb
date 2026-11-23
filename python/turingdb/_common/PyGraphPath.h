#pragma once

#include <stddef.h>
#include <stdint.h>

#include <nanobind/nanobind.h>

namespace pybindings {

namespace nb = nanobind;

// turingdb.GraphPath: a path as a view of its IDs in a read-only numpy uint64 array, which
// it keeps alive. Not a nanobind class, which would add an entry per path to nanobind's
// instance map. _turingproto defines the type and _turingembedded imports it, so both
// modules make one type.
class PyGraphPath {
public:
    static void define(nb::module_& module);
    static void importFrom(const char* moduleName);

    // The path whose nodeCount nodes start at ids[offset], blockData being the IDs of block
    static nb::object create(const nb::object& block, const uint64_t* blockData, size_t offset, size_t nodeCount);
};

}
