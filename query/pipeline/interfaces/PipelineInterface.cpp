#include <spdlog/fmt/bundled/format.h>

#include "PipelineBlockInputInterface.h"
#include "PipelineEdgeInputInterface.h"
#include "PipelineNodeInputInterface.h"
#include "PipelineNodeOutputInterface.h"
#include "PipelineEdgeOutputInterface.h"
#include "PipelineBlockOutputInterface.h"
#include "PipelineValuesInputInterface.h"
#include "PipelineValuesOutputInterface.h"
#include "PipelineValueOutputInterface.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "PipelineException.h"

using namespace db;
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

void PipelineOutputInterface::connectTo(PipelineValuesInputInterface& input) {
    throw PipelineException(fmt::format("{}: cannot connect to values input", PipelineInterfaceKindName::value(getKind())));
}

void PipelineOutputInterface::rename(std::string_view name) {
    throw PipelineException(fmt::format("{}: cannot rename", PipelineInterfaceKindName::value(getKind())));
}

void PipelineInputInterface::propagateColumns(PipelineOutputInterface& output) const {
    const Dataframe* inDf = getDataframe();
    Dataframe* outDf = output.getDataframe();

    for (const auto& col : inDf->cols()) {
        if (outDf->getColumn(col->getTag())) {
            continue;
        }

        outDf->addColumn(col);
    }
}

void PipelineEdgeOutputInterface::rename(std::string_view name) {
    _edgeIDs->rename(name);
}

void PipelineEdgeOutputInterface::renameOtherIDs(std::string_view name) {
    _otherNodes->rename(name);
}

void PipelineValuesOutputInterface::rename(std::string_view name) {
    _values->rename(name);
}

void PipelineValueOutputInterface::rename(std::string_view name) {
    _value->rename(name);
}

// Node Output
void PipelineNodeOutputInterface::rename(std::string_view name) {
    _nodeIDs->rename(name);
}

void PipelineNodeOutputInterface::connectTo(PipelineNodeInputInterface& input) {
    input.setStream(EntityOutputStream::createNodeStream(_nodeIDs->getTag()));
    input.setNodeIDs(_nodeIDs);
    _port->connectTo(input.getPort());
}

void PipelineNodeOutputInterface::connectTo(PipelineBlockInputInterface& input) {
    input.setStream(EntityOutputStream::createNodeStream(_nodeIDs->getTag()));
    _port->connectTo(input.getPort());
}

// Edge Output
void PipelineEdgeOutputInterface::connectTo(PipelineEdgeInputInterface& input) {
    input.setStream(EntityOutputStream::createEdgeStream(_edgeIDs->getTag(),
                                                         _otherNodes->getTag(),
                                                         _edgeTypes->getTag()));
    input.setEdges(_edgeIDs, _otherNodes, _edgeTypes);
    _port->connectTo(input.getPort());
}

void PipelineEdgeOutputInterface::connectTo(PipelineNodeInputInterface& input) {
    input.setStream(EntityOutputStream::createNodeStream(_otherNodes->getTag()));
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
    input.setStream(_stream);
    _port->connectTo(input.getPort());
}

void PipelineBlockOutputInterface::connectTo(PipelineNodeInputInterface& input) {
    input.setStream(_stream);

    const ColumnTag nodeIDsTag = _stream.asNodeStream()._nodeIDsTag;
    const Dataframe* df = _port->getBuffer()->getDataframe();

    if (!nodeIDsTag.isValid()) {
        throw PipelineException("PipelineBlockOutputInterface: nodeIDs column is not defined");
    }

    input.setNodeIDs(df->getColumn(nodeIDsTag));
    _port->connectTo(input.getPort());
}

void PipelineBlockOutputInterface::connectTo(PipelineEdgeInputInterface& input) {
    input.setStream(_stream);

    const auto& stream = _stream.asEdgeStream();
    const ColumnTag edgeIDsTag = stream._edgeIDsTag;
    const ColumnTag otherNodesTag = stream._otherIDsTag;
    const ColumnTag edgeTypesTag = stream._edgeTypesTag;

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
    _port->connectTo(input.getPort());
}

void PipelineBlockOutputInterface::rename(std::string_view name) {
    const Dataframe* df = _port->getBuffer()->getDataframe();

    ColumnTag srcColTag;
    if (_stream.isNodeStream()) {
        srcColTag = _stream.asNodeStream()._nodeIDsTag;
    } else if (_stream.isEdgeStream()) {
        srcColTag = _stream.asEdgeStream()._edgeIDsTag;
    } else {
        throw PipelineException("PipelineBlockOutputInterface: rename on a block is only possible if on a node stream or edge stream");
    }

    NamedColumn* col = df->getColumn(srcColTag);
    if (!col) {
        throw PipelineException(
                fmt::format("PipelineBlockOutputInterface: can not find column tag {} in dataframe",
                srcColTag.getValue()));
    }

    col->rename(name);
}

// Value output
void PipelineValueOutputInterface::connectTo(PipelineBlockInputInterface& input) {
    input.setStream(_stream);
    _port->connectTo(input.getPort());
}

// Values output
void PipelineValuesOutputInterface::connectTo(PipelineBlockInputInterface& input) {
    input.setStream(_stream);
    _port->connectTo(input.getPort());
}

void PipelineValuesOutputInterface::connectTo(PipelineNodeInputInterface& input) {
    input.setStream(_stream);
    const auto& stream = _stream.asNodeStream();
    const ColumnTag nodeIDsTag = stream._nodeIDsTag;
    const Dataframe* df = _port->getBuffer()->getDataframe();

    if (!nodeIDsTag.isValid()) {
        throw PipelineException("PipelineValuesOutputInterface: indices column is not defined");
    }

    input.setNodeIDs(df->getColumn(nodeIDsTag));
    _port->connectTo(input.getPort());
}

void PipelineValuesOutputInterface::connectTo(PipelineValuesInputInterface& input) {
    input.setStream(_stream);
    _port->connectTo(input.getPort());
}

void PipelineValuesOutputInterface::connectTo(PipelineEdgeInputInterface& input) {
    input.setStream(_stream);
    const auto& stream = _stream.asEdgeStream();
    const ColumnTag edgeIDsTag = stream._edgeIDsTag;
    const ColumnTag otherNodesTag = stream._otherIDsTag;
    const ColumnTag edgeTypesTag = stream._edgeTypesTag;

    const Dataframe* df = _port->getBuffer()->getDataframe();

    if (!edgeIDsTag.isValid()) {
        throw PipelineException("PipelineValuesOutputInterface: edgeIDs column is not defined");
    }

    if (!otherNodesTag.isValid()) {
        throw PipelineException("PipelineValuesOutputInterface: otherNodes column is not defined");
    }

    if (!edgeTypesTag.isValid()) {
        throw PipelineException("PipelineValuesOutputInterface: edgeTypes column is not defined");
    }

    input.setEdges(df->getColumn(edgeIDsTag),
                   df->getColumn(otherNodesTag),
                   df->getColumn(edgeTypesTag));
    _port->connectTo(input.getPort());
}

