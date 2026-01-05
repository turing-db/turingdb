#include "ScanNodesProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "PipelineV2.h"
#include "iterators/ScanNodesIterator.h"
#include "columns/ColumnIDs.h"
#include "dataframe/NamedColumn.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"

using namespace db::v2;

ScanNodesProcessor::ScanNodesProcessor()
{
}

ScanNodesProcessor::~ScanNodesProcessor() {
}

std::string ScanNodesProcessor::describe() const {
    return fmt::format("ScanNodesProcessor @={}", fmt::ptr(this));
}

ScanNodesProcessor* ScanNodesProcessor::create(PipelineV2* pipeline) {
    ScanNodesProcessor* scanNodes = new ScanNodesProcessor();

    PipelineOutputPort* outNodeIDs = PipelineOutputPort::create(pipeline, scanNodes);
    scanNodes->_outNodeIDs.setPort(outNodeIDs);
    scanNodes->addOutput(outNodeIDs);

    scanNodes->postCreate(pipeline);
    return scanNodes;
}

void ScanNodesProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(_outNodeIDs.getNodeIDs()->getColumn());
    _it = std::make_unique<ScanNodesChunkWriter>(ctxt->getGraphView());
    _it->setNodeIDs(nodeIDs);

    markAsPrepared();
}

void ScanNodesProcessor::reset() {
    _it->reset();
}

void ScanNodesProcessor::execute() {
    _it->fill(_ctxt->getChunkSize());

    if (!_it->isValid()) {
        finish();
    }

    _outNodeIDs.getPort()->writeData();
}
