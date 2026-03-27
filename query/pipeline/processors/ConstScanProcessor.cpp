#include "ConstScanProcessor.h"

#include <algorithm>

#include <spdlog/fmt/fmt.h>
#include <type_traits>

#include "BioAssert.h"
#include "ID.h"
#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "dataframe/NamedColumn.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"

using namespace db;

template <typename T>
ConstScanProcessor<T>::ConstScanProcessor(std::span<const T> values)
    : _values(values)
{
}

template <typename T>
ConstScanProcessor<T>::~ConstScanProcessor() {
}

template <typename T>
std::string ConstScanProcessor<T>::describe() const {
    return fmt::format("ConstScanProcessor n={} @={}", _values.size(), fmt::ptr(this));
}

template <typename T>
ConstScanProcessor<T>* ConstScanProcessor<T>::create(PipelineV2* pipeline,
                                                     std::span<const T> values) {
    ConstScanProcessor<T>* proc = new ConstScanProcessor<T>(values);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proc);
    proc->_output.setPort(output);
    proc->addOutput(output);

    proc->postCreate(pipeline);
    return proc;
}

template <typename T>
void ConstScanProcessor<T>::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    // Filter out deleted/invalid IDs and sort for cache-friendly access.
    _sortedValues.clear();
    _sortedValues.reserve(_values.size());

    // Specialisation for Node/EdgeIDs: ensures all are valid
    if constexpr (TypedInternalID<T>) {
        const GraphReader reader = _ctxt->getGraphView().read();

        for (const T v : _values) {
            bool entityExists = false;
            if constexpr (std::is_same_v<NodeID, T>) {
                entityExists = reader.graphHasNode(v);
            } else if constexpr (std::is_same_v<EdgeID, T>) {
                entityExists = reader.graphHasEdge(v);
            }

            if (entityExists) {
                _sortedValues.push_back(v);
            }
        }
    }

    // FIXME: Does it make sense to sort in the general case?
    std::sort(_sortedValues.begin(), _sortedValues.end());

    ColumnValues* values = _output.getValues()->as<ColumnValues>();
    bioassert(values, "Null values in output");

    _outCol = values;
    _offset = 0;

    markAsPrepared();
}

template <typename T>
void ConstScanProcessor<T>::reset() {
    _offset = 0;
    markAsReset();
}

template <typename T>
void ConstScanProcessor<T>::execute() {
    _outCol->clear();

    const size_t remaining = _sortedValues.size() - _offset;
    const size_t chunkSize = std::min(remaining, _ctxt->getChunkSize());

    _outCol->resize(chunkSize);
    std::copy_n(_sortedValues.data() + _offset, chunkSize, _outCol->data());
    _offset += chunkSize;

    _output.getPort()->writeData();

    if (_offset >= _sortedValues.size()) {
        finish();
    }
}

namespace db {
template class ConstScanProcessor<NodeID>;
}
