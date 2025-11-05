#include "PipelineInterface.h"

#include "PipelineException.h"

using namespace db::v2;

void PipelineBlockOutputInterface::connectTo(PipelineNodeInputInterface& input) {
    throw PipelineException("PipelineBlockOutputInterface: cannot connect to node input");
}

void PipelineBlockOutputInterface::connectTo(PipelineEdgeInputInterface& input) {
    throw PipelineException("PipelineBlockOutputInterface: cannot connect to edge input");
}

void PipelineNodeOutputInterface::connectTo(PipelineEdgeInputInterface& input) {
    throw PipelineException("PipelineNodeOutputInterface: cannot connect to edge input");
}

void PipelineValuesOutputInterface::connectTo(PipelineNodeInputInterface& input) {
    throw PipelineException("PipelineValuesOutputInterface: cannot connect to node input");
}

void PipelineValuesOutputInterface::connectTo(PipelineEdgeInputInterface& input) {
    throw PipelineException("PipelineValuesOutputInterface: cannot connect to edge input");
}

void PipelineValueOutputInterface::connectTo(PipelineNodeInputInterface& input) {
    throw PipelineException("PipelineValueOutputInterface: cannot connect to node input");
}

void PipelineValueOutputInterface::connectTo(PipelineEdgeInputInterface& input) {
    throw PipelineException("PipelineValueOutputInterface: cannot connect to edge input");
}
