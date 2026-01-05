#include "GetLabelSetIDProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnIDs.h"
#include "dataframe/NamedColumn.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"

#include "PipelinePort.h"
#include "ExecutionContext.h"

using namespace db;

GetLabelSetIDProcessor::GetLabelSetIDProcessor()
{
}

GetLabelSetIDProcessor::~GetLabelSetIDProcessor() {
}

GetLabelSetIDProcessor* GetLabelSetIDProcessor::create(PipelineV2* pipeline) {
    GetLabelSetIDProcessor* getlabel = new GetLabelSetIDProcessor();

    PipelineInputPort* inNodeIDs = PipelineInputPort::create(pipeline, getlabel);
    getlabel->_input.setPort(inNodeIDs);
    getlabel->addInput(inNodeIDs);

    PipelineOutputPort* outValues = PipelineOutputPort::create(pipeline, getlabel);
    getlabel->_output.setPort(outValues);
    getlabel->addOutput(outValues);

    getlabel->postCreate(pipeline);

    return getlabel;
}

std::string GetLabelSetIDProcessor::describe() const {
    return fmt::format("GetLabelSetIDProcessor @={}", fmt::ptr(this));
}

void GetLabelSetIDProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    _nodeIDs = dynamic_cast<ColumnNodeIDs*>(_input.getNodeIDs()->getColumn());
    _labelsetIDs = dynamic_cast<ColumnVector<LabelSetID>*>(_output.getValues()->getColumn());

    markAsPrepared();
}

void GetLabelSetIDProcessor::reset() {
}

void GetLabelSetIDProcessor::execute() {
    const GraphView& view = _ctxt->getGraphView();
    const GraphReader reader = view.read();

    LabelSetIDs& labelsetIDs = *_labelsetIDs;
    const ColumnNodeIDs& nodeIDs = *_nodeIDs;

    labelsetIDs.resize(nodeIDs.size());
    size_t i = 0;
    for (NodeID nodeID : nodeIDs) {
        labelsetIDs.set(i, reader.getNodeLabelSet(nodeID).getID());
        i++;
    }

    _input.getPort()->consume();
    _output.getPort()->writeData();
    finish();
}
