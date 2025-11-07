#include "PipelineInterface.h"

#include "PipelineException.h"

using namespace db;
using namespace db::v2;

NamedColumn* PipelineOutputInterface::getIndices() const {
    throw PipelineException("PipelineOutputInterface: cannot get indices");
}

NamedColumn* PipelineOutputInterface::getNodeIDs() const {
    throw PipelineException("PipelineOutputInterface: cannot get node IDs");
}

NamedColumn* PipelineOutputInterface::getEdgeIDs() const {
    throw PipelineException("PipelineOutputInterface: cannot get edge IDs");
}

NamedColumn* PipelineOutputInterface::getTargetNodes() const {
    throw PipelineException("PipelineOutputInterface: cannot get target nodes");
}

NamedColumn* PipelineOutputInterface::getEdgeTypes() const {
    throw PipelineException("PipelineOutputInterface: cannot get edge types");
}

NamedColumn* PipelineOutputInterface::getValues() const {
    throw PipelineException("PipelineOutputInterface: cannot get values");
}

NamedColumn* PipelineOutputInterface::getValue() const {
    throw PipelineException("PipelineOutputInterface: cannot get value");
}

void PipelineOutputInterface::connectTo(PipelineNodeInputInterface& input) {
    throw PipelineException("PipelineOutputInterface: cannot connect to node input");
}

void PipelineOutputInterface::connectTo(PipelineBlockInputInterface& input) {
    throw PipelineException("PipelineOutputInterface: cannot connect to block input");
}

void PipelineOutputInterface::connectTo(PipelineEdgeInputInterface& input) {
    throw PipelineException("PipelineOutputInterface: cannot connect to edge input");
}
