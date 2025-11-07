#include "PipelineInterface.h"

#include "dataframe/NamedColumn.h"

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

void PipelineBlockOutputInterface::rename(const std::string_view& name) {
    throw PipelineException("PipelineBlockOutputInterface: cannot rename");
}

void PipelineNodeOutputInterface::rename(const std::string_view& name) {
    _nodeIDs->getHeader().setName(name);
}

void PipelineEdgeOutputInterface::rename(const std::string_view& name) {
    _otherNodes->getHeader().setName(name);
}

void PipelineValuesOutputInterface::rename(const std::string_view& name) {
    _values->getHeader().setName(name);
}

void PipelineValueOutputInterface::rename(const std::string_view& name) {
    _value->getHeader().setName(name);
}
