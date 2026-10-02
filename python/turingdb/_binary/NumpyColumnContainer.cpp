#include "NumpyColumnContainer.h"

#include <nanobind/stl/string.h>

#include "NanobindUtils.h"
#include "NumpyColumn.h"

using namespace pybindings;

NumpyColumnContainer::NumpyColumnContainer()
{
}

NumpyColumnContainer::~NumpyColumnContainer() {
}

void NumpyColumnContainer::addColumn(NumpyColumn* column, std::string_view name) {
    _columns.emplace_back(column);
    _names.emplace_back(name);
}

void NumpyColumnContainer::finishChunk() {
    _rowCount += _chunkRowCount;
    _chunkRowCount = 0;

    for (const std::unique_ptr<NumpyColumn>& column : _columns) {
        column->finishChunk();
    }
}

nb::dict NumpyColumnContainer::toPython() {
    nb::dict data;
    nb::dict dtypes;
    const ValueToPyObject visitor;

    for (size_t columnIndex = 0; columnIndex < _columns.size(); columnIndex++) {
        const std::string& name = _names[columnIndex];
        const std::string key = name.empty() ? "$" + std::to_string(columnIndex) : name;

        NumpyColumn* column = _columns[columnIndex].get();

        const nb::str pyKey(key.data(), key.size());
        data[pyKey] = column->toPython(_rowCount, visitor);
        dtypes[pyKey] = column->getDtypeName();
    }

    nb::dict envelope;
    envelope["data"] = data;
    envelope["dtypes"] = dtypes;
    return envelope;
}
