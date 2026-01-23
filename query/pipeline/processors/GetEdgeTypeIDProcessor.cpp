#include "GetEdgeTypeIDProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "ExecutionContext.h"
#include "FatalException.h"
#include "ID.h"
#include "PipelinePort.h"
#include "columns/ColumnIDs.h"
#include "dataframe/NamedColumn.h"
#include "reader/GraphReader.h"
#include "views/GraphView.h"

using namespace db;

GetEdgeTypeIDProcessor::GetEdgeTypeIDProcessor()
{
}

GetEdgeTypeIDProcessor::~GetEdgeTypeIDProcessor() {
}

GetEdgeTypeIDProcessor* GetEdgeTypeIDProcessor::create(PipelineV2* pipeline) {
    auto* proc = new GetEdgeTypeIDProcessor();

    {
        PipelineInputPort* inputEdgeIDs = PipelineInputPort::create(pipeline, proc);
        proc->_input.setPort(inputEdgeIDs);
        proc->addInput(inputEdgeIDs);
    }

    {
        PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proc);
        proc->_output.setPort(output);
        proc->addOutput(output);
    }

    proc->postCreate(pipeline);

    return proc;
}

std::string GetEdgeTypeIDProcessor::describe() const {
    return fmt::format("GetEdgeTypeIDProcessor @={}", fmt::ptr(this));
}

void GetEdgeTypeIDProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    {
        NamedColumn* namedEdgeCol = _input.getEdgeIDs();
        if (!namedEdgeCol) {
            throw FatalException("Failed to get input to GetEdgeTypeIDProcessor.");
        }

        auto* edgeIDCol = dynamic_cast<ColumnEdgeIDs*>(namedEdgeCol->getColumn());
        if (!edgeIDCol) {
            throw FatalException(
                "Failed to get EdgeIDs as input to GetEdgeTypeIDProcessor.");
        }

        _edgeIDs = edgeIDCol;
    }

    {
        NamedColumn* outputNamedCol = _output.getValues();
        if (!outputNamedCol) {
            throw FatalException(
                "Failed to get output column for GetEdgeTypeIDProcessor.");
        }

        auto* edgeTypeCol =
            dynamic_cast<ColumnVector<EdgeTypeID>*>(outputNamedCol->getColumn());
        if (!edgeTypeCol) {
            throw FatalException(
                "Failed to get EdgeTypeIDs output of GetEdgeTypeIDProcessor.");
        }

        _edgeTypeIDs = edgeTypeCol;
    }

    markAsPrepared();
}

void GetEdgeTypeIDProcessor::reset() {
    markAsReset();
}

void GetEdgeTypeIDProcessor::execute() {
    const GraphView& view = _ctxt->getGraphView();
    const GraphReader reader = view.read();

    const ColumnEdgeIDs& edgeIDs = *_edgeIDs;
    EdgeTypeIDs& edgeTypeIDs = *_edgeTypeIDs;
    
    edgeTypeIDs.resize(edgeIDs.size());

    for (size_t i {0}; EdgeID edge : edgeIDs) {
        edgeTypeIDs.set(i++, reader.getEdgeTypeID(edge));
    }

    // Always finishes in a single chunk
    _input.getPort()->consume();
    _output.getPort()->writeData();
    finish();
}
