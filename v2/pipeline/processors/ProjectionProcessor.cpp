#include "ProjectionProcessor.h"

using namespace db::v2;

ProjectionProcessor::ProjectionProcessor()
{
}

ProjectionProcessor::~ProjectionProcessor() {
}

ProjectionProcessor* ProjectionProcessor::create(PipelineV2* pipeline) {
    ProjectionProcessor* proj = new ProjectionProcessor();

    PipelineInputPort* input = PipelineInputPort::create(pipeline, proj);
    proj->_input.setPort(input);
    proj->addInput(input);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proj);
    proj->_output.setPort(output);
    proj->addOutput(output);

    proj->postCreate(pipeline);

    return proj;
}

void ProjectionProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void ProjectionProcessor::reset() {
    markAsReset();
}

void ProjectionProcessor::execute() {
    PipelineInputPort* inputPort = _input.getPort();
    inputPort->consume();

    PipelineOutputPort* outputPort = _output.getPort();
    outputPort->writeData();

    finish();
}
