#pragma once

#include "EntityOutputStream.h"
#include "PipelineInputInterface.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineBlockInputInterface : public PipelineInputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::BLOCK; }

    void setStream(const EntityOutputStream& stream) {
        _stream = stream;
    }

    const EntityOutputStream& getStream() const {
        return _stream;
    }

private:
    EntityOutputStream _stream;
};

}
