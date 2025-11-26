#pragma once

#include "EntityOutputStream.h"
#include "EnumToString.h"
#include "PipelinePort.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineInputPort;
class PipelineOutputPort;
class PipelineNodeInputInterface;
class PipelineBlockInputInterface;
class PipelineEdgeInputInterface;
class PipelineValuesInputInterface;
class PipelineOutputInterface;

enum class PipelineInterfaceKind {
    NODE,
    EDGE,
    BLOCK,
    VALUES,
    VALUE,

    _SIZE
};

using PipelineInterfaceKindName = EnumToString<PipelineInterfaceKind>::Create<
    EnumStringPair<PipelineInterfaceKind::NODE, "node">,
    EnumStringPair<PipelineInterfaceKind::EDGE, "edge">,
    EnumStringPair<PipelineInterfaceKind::BLOCK, "block">,
    EnumStringPair<PipelineInterfaceKind::VALUES, "values">,
    EnumStringPair<PipelineInterfaceKind::VALUE, "value">>;

class PipelineInputInterface {
public:
    virtual PipelineInterfaceKind getKind() const = 0;

    PipelineInputPort* getPort() const { return _port; }
    Dataframe* getDataframe() const { return _port->getBuffer()->getDataframe(); }

    void setPort(PipelineInputPort* port) { _port = port; }

    void propagateColumns(PipelineOutputInterface& output) const;

    void setStream(const EntityOutputStream& stream) {
        _stream = stream;
    }

    const EntityOutputStream& getStream() const {
        return _stream;
    }

protected:
    PipelineInputPort* _port {nullptr};
    EntityOutputStream _stream;

    PipelineInputInterface() = default;
    virtual ~PipelineInputInterface() = default;
};

}
