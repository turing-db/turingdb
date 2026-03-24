#include "ConstScanProcessor.h"

#include <algorithm>

#include <spdlog/fmt/fmt.h>

#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "columns/ColumnIDs.h"
#include "dataframe/NamedColumn.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"

using namespace db;

ConstScanProcessor::ConstScanProcessor(std::span<const NodeID> nodeIDs)
    : _nodeIDs(nodeIDs)
{
}

ConstScanProcessor::~ConstScanProcessor() {
}

std::string ConstScanProcessor::describe() const {
    return fmt::format("ConstScanProcessor n={} @={}", _nodeIDs.size(), fmt::ptr(this));
}

ConstScanProcessor* ConstScanProcessor::create(PipelineV2* pipeline,
                                               std::span<const NodeID> nodeIDs) {
    ConstScanProcessor* proc = new ConstScanProcessor(nodeIDs);

    PipelineOutputPort* outNodeIDs = PipelineOutputPort::create(pipeline, proc);
    proc->_outNodeIDs.setPort(outNodeIDs);
    proc->addOutput(outNodeIDs);

    proc->postCreate(pipeline);
    return proc;
}

void ConstScanProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    // Filter out deleted/invalid IDs and sort for cache-friendly access.
    const GraphReader reader = _ctxt->getGraphView().read();
    _sortedNodeIDs.clear();
    _sortedNodeIDs.reserve(_nodeIDs.size());
    for (const NodeID nid : _nodeIDs) {
        if (reader.graphHasNode(nid)) {
            _sortedNodeIDs.push_back(nid);
        }
    }
    std::sort(_sortedNodeIDs.begin(), _sortedNodeIDs.end());

    _outCol = static_cast<ColumnNodeIDs*>(_outNodeIDs.getNodeIDs()->getColumn());
    _offset = 0;

    markAsPrepared();
}

void ConstScanProcessor::reset() {
    _offset = 0;
    markAsReset();
}

void ConstScanProcessor::execute() {
    _outCol->clear();

    const size_t remaining = _sortedNodeIDs.size() - _offset;
    const size_t chunkSize = std::min(remaining, _ctxt->getChunkSize());

    _outCol->resize(chunkSize);
    std::copy_n(_sortedNodeIDs.data() + _offset, chunkSize, _outCol->data());
    _offset += chunkSize;

    if (_offset >= _sortedNodeIDs.size()) {
        finish();
    }

    _outNodeIDs.getPort()->writeData();
}
