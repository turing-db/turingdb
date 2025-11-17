#pragma once

#include "PipelineBlockInputInterface.h"
#include "PipelineOutputInterface.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineBlockOutputInterface : public PipelineOutputInterface {
public:
    PipelineBlockOutputInterface() = default;

    static PipelineBlockOutputInterface createOutputOnly() {
        return {};
    }

    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::BLOCK; }

    void connectTo(PipelineBlockInputInterface& input) override;
    void connectTo(PipelineNodeInputInterface& input) override;
    void connectTo(PipelineEdgeInputInterface& input) override;

    const EntityOutputStream& getStream() const { return _stream; }

private:
    EntityOutputStream _stream;
};

}
