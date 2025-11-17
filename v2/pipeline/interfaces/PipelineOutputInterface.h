#pragma once

#include <string_view>

#include "EntityOutputStream.h"
#include "PipelineInputInterface.h"
#include "PipelinePort.h"

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

    virtual void rename(const std::string_view& name);

protected:
    PipelineOutputPort* _port {nullptr};

    PipelineOutputInterface() = default;
    virtual ~PipelineOutputInterface() = default;
};

}
