#include "ConstWriteSourceProcessor.h"

#include <algorithm>

#include <spdlog/fmt/fmt.h>

#include "ID.h"
#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/AllowedKinds.h"
#include "dataframe/NamedColumn.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"

#include "BioAssert.h"

using namespace db;

namespace {

// Identify the concrete ColumnVector type of the values column so we can
// reorder it through the non-const _values member stored in the processor.
struct ReorderValues {
    template <typename T>
    void operator()(const ColumnVector<T>*) {
        auto* src = dynamic_cast<ColumnVector<T>*>(_values);
        ColumnVector<T> reordered;
        reordered.reserve(_perm.size());
        for (const size_t idx : _perm) {
            reordered.emplace_back((*src)[idx]);
        }
        *src = std::move(reordered);
    }

    Column* _values {nullptr};
    const std::vector<size_t>& _perm;
};

struct CopyChunk {
    template <typename T>
    void operator()(const ColumnVector<T>* src) {
        auto* dst = dynamic_cast<ColumnVector<T>*>(_out);
        const size_t remaining = src->size() - _offset;
        const size_t canWrite = std::min(remaining, _chunkSize);
        dst->resize(canWrite);
        std::copy_n(src->begin() + _offset, canWrite, dst->data());
    }

    Column* _out {nullptr};
    size_t _offset {0};
    size_t _chunkSize {0};
};

}

ConstWriteSourceProcessor::ConstWriteSourceProcessor(Column* nodeIDs, Column* values)
    : _nodeIDs(nodeIDs),
    _values(values)
{
}

ConstWriteSourceProcessor::~ConstWriteSourceProcessor() {
}

std::string ConstWriteSourceProcessor::describe() const {
    return fmt::format("ConstWriteSourceProcessor n={} @={}", _nodeIDs->size(), fmt::ptr(this));
}

ConstWriteSourceProcessor* ConstWriteSourceProcessor::create(PipelineV2* pipeline,
                                                             Column* nodeIDs,
                                                             Column* values) {
    auto* proc = new ConstWriteSourceProcessor(nodeIDs, values);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proc);
    proc->_output.setPort(output);
    proc->addOutput(output);

    proc->postCreate(pipeline);
    return proc;
}

void ConstWriteSourceProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    bioassert(_nodeIDs, "Null nodeIDs column");
    bioassert(_values, "Null values column");

    auto* nodeCol = dynamic_cast<ColumnNodeIDs*>(_nodeIDs);
    bioassert(nodeCol, "nodeIDs must be ColumnNodeIDs");

    const GraphReader reader = ctxt->getGraphView().read();
    std::vector<NodeID>& raw = nodeCol->getRaw();

    // Build index permutation, filter out invalid NodeIDs
    std::vector<size_t> perm;
    perm.reserve(raw.size());
    for (size_t i = 0; i < raw.size(); ++i) {
        if (reader.graphHasNode(raw[i])) {
            perm.push_back(i);
        }
    }

    // Apply permutation to NodeIDs
    ColumnNodeIDs sortedNodeIDs;
    sortedNodeIDs.reserve(perm.size());
    for (const size_t idx : perm) {
        sortedNodeIDs.emplace_back(raw[idx]);
    }
    *nodeCol = std::move(sortedNodeIDs);

    // Apply same permutation to values column
    using ValTypes = WriteProcessorPropertyTypes;
    using Dispatcher = ColumnSingleDispatcher<ValTypes::AllowedVector, ReorderValues, ValTypes::ExcludedVector>;

    ReorderValues reorder {._values = _values, ._perm = perm};
    Dispatcher::dispatch(_values, reorder);

    _offset = 0;
    markAsPrepared();
}

void ConstWriteSourceProcessor::reset() {
    _offset = 0;
    markAsReset();
}

void ConstWriteSourceProcessor::execute() {
    bioassert(_nodeIDOutputCol, "Null nodeID output column");
    bioassert(_valuesOutputCol, "Null values output column");

    const size_t chunkSize = _ctxt->getChunkSize();

    // Copy NodeID chunk
    {
        auto* src = dynamic_cast<const ColumnNodeIDs*>(_nodeIDs);
        auto* dst = dynamic_cast<ColumnNodeIDs*>(_nodeIDOutputCol->getColumn());
        const size_t remaining = src->size() - _offset;
        const size_t canWrite = std::min(remaining, chunkSize);
        dst->resize(canWrite);
        std::copy_n(src->begin() + _offset, canWrite, dst->data());
    }

    // Copy values chunk
    {
        using ValTypes = WriteProcessorPropertyTypes;
        using Dispatcher = ColumnSingleDispatcher<ValTypes::AllowedVector, CopyChunk, ValTypes::ExcludedVector>;

        CopyChunk copier {._out = _valuesOutputCol->getColumn(),
                          ._offset = _offset,
                          ._chunkSize = chunkSize};
        Dispatcher::dispatch(_values, copier);
    }

    const size_t remaining = _nodeIDs->size() - _offset;
    const size_t written = std::min(remaining, chunkSize);
    _offset += written;

    _output.getPort()->writeData();

    if (_offset >= _nodeIDs->size()) {
        finish();
    }
}
