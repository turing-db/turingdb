#pragma once

#include "PipelineInputInterface.h"

namespace db {
class NamedColumn;
}

namespace db {

class PipelineBlockInputInterface : public PipelineInputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::BLOCK; }
};

}
