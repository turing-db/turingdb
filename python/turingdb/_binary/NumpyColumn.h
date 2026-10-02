#pragma once

#include <stddef.h>

#include <nanobind/nanobind.h>

namespace pybindings {

namespace nb = nanobind;

struct ValueToPyObject;

// A decoded result column. Every dataframe of a response appends to the same column, so
// it holds the whole result when the response ends.
class NumpyColumn {
public:
    NumpyColumn();
    virtual ~NumpyColumn();

    // The next dataframe's rows go after the ones decoded so far.
    virtual void finishChunk() = 0;

    // Hands the decoded rows to Python. The column is empty afterwards.
    virtual nb::object toPython(size_t rowCount, const ValueToPyObject& visitor) = 0;

    virtual const char* getDtypeName() const = 0;
};

}
