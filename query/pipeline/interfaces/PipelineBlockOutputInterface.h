#pragma once

#include "PipelineBlockInputInterface.h"
#include "PipelineOutputInterface.h"

namespace db {
class NamedColumn;
}

namespace db {

class PipelineBlockOutputInterface : public PipelineOutputInterface {
public:
    PipelineBlockOutputInterface() = default;

    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::BLOCK; }

    void connectTo(PipelineBlockInputInterface& input) override;
    void connectTo(PipelineNodeInputInterface& input) override;
    void connectTo(PipelineEdgeInputInterface& input) override;
    void rename(std::string_view name) override;

    void setStream(const EntityOutputStream& stream) { _stream = stream; }

    const EntityOutputStream& getStream() const { return _stream; }
};

}
