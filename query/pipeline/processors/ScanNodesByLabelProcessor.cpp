#include "ScanNodesByLabelProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "PipelineV2.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "ExecutionContext.h"
#include "columns/ColumnIDs.h"
#include "metadata/LabelSetHandle.h"

using namespace db;

ScanNodesByLabelProcessor::ScanNodesByLabelProcessor(const LabelSet* labelset)
    : _labelset(labelset)
{
}

ScanNodesByLabelProcessor::~ScanNodesByLabelProcessor() {
}

std::string ScanNodesByLabelProcessor::describe() const {
    return fmt::format("ScanNodesByLabelProcessor @={}", fmt::ptr(this));
}

ScanNodesByLabelProcessor* ScanNodesByLabelProcessor::create(PipelineV2* pipeline, const LabelSet* labelset) {
    ScanNodesByLabelProcessor* scanNodes = new ScanNodesByLabelProcessor(labelset);

    PipelineOutputPort* outNodeIDs = PipelineOutputPort::create(pipeline, scanNodes);
    scanNodes->_outNodeIDs.setPort(outNodeIDs);
    scanNodes->addOutput(outNodeIDs);

    scanNodes->postCreate(pipeline);
    return scanNodes;
}

void ScanNodesByLabelProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(_outNodeIDs.getNodeIDs()->getColumn());

    _it = std::make_unique<ScanNodesByLabelChunkWriter>(ctxt->getGraphView(), LabelSetHandle(*_labelset));
    _it->setNodeIDs(nodeIDs);

    markAsPrepared();
}

void ScanNodesByLabelProcessor::reset() {
    _it->reset();
}

void ScanNodesByLabelProcessor::execute() {
    _it->fill(_ctxt->getChunkSize());

    if (!_it->isValid()) {
        finish();
    }

    _outNodeIDs.getPort()->writeData();
}
