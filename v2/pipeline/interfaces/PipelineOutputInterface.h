#pragma once

#include <string_view>

#include "PipelineInputInterface.h"
#include "PipelinePort.h"

#include "EntityOutputStream.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineOutputInterface {
public:
    virtual PipelineInterfaceKind getKind() const = 0;

    PipelineOutputPort* getPort() const { return _port; }
    Dataframe* getDataframe() const { return _port->getBuffer()->getDataframe(); }

    void setPort(PipelineOutputPort* port) { _port = port; }

    virtual void connectTo(PipelineNodeInputInterface& input);
    virtual void connectTo(PipelineBlockInputInterface& input);
    virtual void connectTo(PipelineEdgeInputInterface& input);
    virtual void connectTo(PipelineValuesInputInterface& input);

    virtual void rename(const std::string_view& name);

    void setStream(const EntityOutputStream& stream) { _stream = stream; }

    const EntityOutputStream& getStream() const { return _stream; }
    EntityOutputStream& getStream() { return _stream; }

protected:
    PipelineOutputPort* _port {nullptr};
    EntityOutputStream _stream;

    PipelineOutputInterface() = default;
    virtual ~PipelineOutputInterface() = default;
};

}
