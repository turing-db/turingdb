#include <spdlog/fmt/bundled/format.h>

#include "PipelineBlockInputInterface.h"
#include "PipelineEdgeInputInterface.h"
#include "PipelineNodeInputInterface.h"
#include "PipelineNodeOutputInterface.h"
#include "PipelineEdgeOutputInterface.h"
#include "PipelineBlockOutputInterface.h"
#include "PipelineValuesOutputInterface.h"
#include "PipelineValueOutputInterface.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "PipelineException.h"

using namespace db::v2;

void PipelineOutputInterface::connectTo(PipelineNodeInputInterface& input) {
    throw PipelineException(fmt::format("{}: cannot connect to node input", PipelineInterfaceKindName::value(getKind())));
}

void PipelineOutputInterface::connectTo(PipelineBlockInputInterface& input) {
    throw PipelineException(fmt::format("{}: cannot connect to block input", PipelineInterfaceKindName::value(getKind())));
}

void PipelineOutputInterface::connectTo(PipelineEdgeInputInterface& input) {
    throw PipelineException(fmt::format("{}: cannot connect to edge input", PipelineInterfaceKindName::value(getKind())));
}

void PipelineOutputInterface::rename(const std::string_view& name) {
    throw PipelineException(fmt::format("{}: cannot rename", PipelineInterfaceKindName::value(getKind())));
}

void PipelineEdgeOutputInterface::rename(const std::string_view& name) {
    _edgeIDs->rename(name);
}

void PipelineEdgeOutputInterface::renameOtherIDs(const std::string_view& name) {
    _otherNodes->rename(name);
}

void PipelineValuesOutputInterface::rename(const std::string_view& name) {
    _values->rename(name);
}

void PipelineValueOutputInterface::rename(const std::string_view& name) {
    _value->rename(name);
}

// Node Output
void PipelineNodeOutputInterface::rename(const std::string_view& name) {
    _nodeIDs->rename(name);
}

void PipelineNodeOutputInterface::connectTo(PipelineNodeInputInterface& input) {
    input.setNodeIDs(_nodeIDs);
    _port->connectTo(input.getPort());
}

void PipelineNodeOutputInterface::connectTo(PipelineBlockInputInterface& input) {
    input.setStream(EntityOutputStream::createNodeStream(_nodeIDs->getTag()));
    _port->connectTo(input.getPort());
}

// Edge Output
void PipelineEdgeOutputInterface::connectTo(PipelineEdgeInputInterface& input) {
    input.setEdges(_edgeIDs, _otherNodes, _edgeTypes);
    _port->connectTo(input.getPort());
}

void PipelineEdgeOutputInterface::connectTo(PipelineNodeInputInterface& input) {
    input.setNodeIDs(_otherNodes);
    _port->connectTo(input.getPort());
}

void PipelineEdgeOutputInterface::connectTo(PipelineBlockInputInterface& input) {
    input.setStream(EntityOutputStream::createEdgeStream(_edgeIDs->getTag(),
                                                         _otherNodes->getTag(),
                                                         _edgeTypes->getTag()));
    _port->connectTo(input.getPort());
}

// Block output
void PipelineBlockOutputInterface::connectTo(PipelineBlockInputInterface& input) {
    _port->connectTo(input.getPort());
}

void PipelineBlockOutputInterface::connectTo(PipelineNodeInputInterface& input) {
    const ColumnTag nodeIDsTag = _stream->getNodeIDsTag();
    const Dataframe* df = _port->getBuffer()->getDataframe();

    if (!nodeIDsTag.isValid()) {
        throw PipelineException("PipelineBlockOutputInterface: nodeIDs column is not defined");
    }

    input.setNodeIDs(df->getColumn(nodeIDsTag));
}

void PipelineBlockOutputInterface::connectTo(PipelineEdgeInputInterface& input) {
    const ColumnTag edgeIDsTag = _stream->getEdgeIDsTag();
    const ColumnTag otherNodesTag = _stream->getOtherIDsTag();
    const ColumnTag edgeTypesTag = _stream->getEdgeTypesTag();

    const Dataframe* df = _port->getBuffer()->getDataframe();

    if (!edgeIDsTag.isValid()) {
        throw PipelineException("PipelineBlockOutputInterface: edgeIDs column is not defined");
    }

    if (!otherNodesTag.isValid()) {
        throw PipelineException("PipelineBlockOutputInterface: otherNodes column is not defined");
    }

    if (!edgeTypesTag.isValid()) {
        throw PipelineException("PipelineBlockOutputInterface: edgeTypes column is not defined");
    }

    input.setEdges(df->getColumn(edgeIDsTag),
                   df->getColumn(otherNodesTag),
                   df->getColumn(edgeTypesTag));
}

// Value output
void PipelineValueOutputInterface::connectTo(PipelineBlockInputInterface& input) {
    _port->connectTo(input.getPort());
}

// Values output
void PipelineValuesOutputInterface::connectTo(PipelineBlockInputInterface& input) {
    _port->connectTo(input.getPort());
}

